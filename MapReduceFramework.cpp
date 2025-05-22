#include <thread>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <iostream>
#include <cstdlib>
#include <memory>
#include "Barrier.h"        
#include "MapReduceFramework.h"
#include "MapReduceClient.h"

struct JobContext;

// Thread context for individual threads
struct ThreadContext {
    int thread_id;
    JobContext* job_context;
};

// JobContext structure - complete implementation
struct JobContext {
    // =================== BASIC JOB PARAMETERS ===================
    int multiThreadLevel;
    const MapReduceClient* client_ptr;
    const InputVec* input_vec_ptr;
    OutputVec* output_vec_ptr;

    // ===== THREAD MANAGEMENT =====
    std::vector<std::thread> worker_threads;
    std::atomic<bool> joined_flag;
    std::vector<ThreadContext> thread_contexts;

    // =================== JOB STATE TRACKING ===================
    std::atomic<uint64_t> job_state_atomic;
    std::mutex state_update_mutex;  // Mutex to ensure atomic state updates
    /*
    Single atomic variable to hold stage, processed count, and total count
    Bits 62-63: stage_t (0-3 for UNDEFINED, MAP, SHUFFLE, REDUCE)
    Bits 31-61: processed_items_count (31 bits)
    Bits 0-30: total_items_for_current_stage (31 bits)
    */

    // ===== ATOMIC COUNTERS =====
    std::atomic<uint32_t> next_input_index;
    std::atomic<uint32_t> next_reduce_index;
    std::atomic<uint32_t> map_completed_count;
    std::atomic<uint32_t> reduce_completed_count;

    // === Synchronization Primitives ===
    std::unique_ptr<Barrier> processing_barrier;           
    std::mutex output_vector_mutex;

    // === Data Structures for Phases ===
    std::vector<IntermediateVec> per_thread_intermediate_vectors;
    std::vector<IntermediateVec> shuffled_vectors;

    // Constructor
    JobContext(int threadLevel, const MapReduceClient* client, 
               const InputVec* input, OutputVec* output) 
        : multiThreadLevel(threadLevel), 
          client_ptr(client), 
          input_vec_ptr(input), 
          output_vec_ptr(output),
          joined_flag(false),
          thread_contexts(threadLevel),
          job_state_atomic(0),
          next_input_index(0),
          next_reduce_index(0),
          map_completed_count(0),
          reduce_completed_count(0),
          processing_barrier(std::make_unique<Barrier>(threadLevel)),
          per_thread_intermediate_vectors(threadLevel) {
        
        worker_threads.reserve(threadLevel);
        
        // Initialize thread contexts
        for (int i = 0; i < threadLevel; ++i) {
            thread_contexts[i] = {i, this};
        }
        
        // Initialize job state to UNDEFINED stage with input size
        uint64_t state = ((uint64_t)UNDEFINED_STAGE << 62) | ((uint64_t)0 << 31) | input->size();
        job_state_atomic.store(state);
    }
    
    // Helper method to update job state - ensures progress never goes backwards
    void updateJobState(stage_t stage, uint32_t processed, uint32_t total) {
        std::lock_guard<std::mutex> lock(state_update_mutex);
        
        uint64_t current_state = job_state_atomic.load();
        stage_t current_stage = (stage_t)(current_state >> 62);
        uint32_t current_processed = (current_state >> 31) & 0x7FFFFFFF;
        
        // Only update if we're advancing
        if (stage > current_stage || 
            (stage == current_stage && processed >= current_processed)) {
            uint64_t new_state = ((uint64_t)stage << 62) | ((uint64_t)processed << 31) | total;
            job_state_atomic.store(new_state);
        }
    }
    
    // Helper method to get current job state
    void getJobState(JobState* state) const {
        uint64_t atomic_value = job_state_atomic.load();
        state->stage = (stage_t)(atomic_value >> 62);
        uint32_t processed = (atomic_value >> 31) & 0x7FFFFFFF;
        uint32_t total = atomic_value & 0x7FFFFFFF;
        
        if (total == 0) {
            state->percentage = 0.0f;
        } else {
            state->percentage = (100.0f * processed) / total;
        }
    }
};

// Worker thread function
void workerThreadFunction(ThreadContext* tc) {
    JobContext* job_ctx = tc->job_context;
    int thread_id = tc->thread_id;
    
    try {
        // =================== MAP PHASE ===================
        uint32_t input_size = job_ctx->input_vec_ptr->size();
        
        // Only thread 0 updates to MAP stage
        if (thread_id == 0) {
            job_ctx->updateJobState(MAP_STAGE, 0, input_size);
        }
        
        while (true) {
            // Atomically get next input item to process
            uint32_t current_index = job_ctx->next_input_index.fetch_add(1);
            
            if (current_index >= input_size) {
                break; // No more items to process
            }
            
            // Process the input pair
            const InputPair& input_pair = (*job_ctx->input_vec_ptr)[current_index];
            job_ctx->client_ptr->map(input_pair.first, input_pair.second, tc);
            
            // Increment completed count and update progress
            uint32_t completed = job_ctx->map_completed_count.fetch_add(1) + 1;
            job_ctx->updateJobState(MAP_STAGE, completed, input_size);
        }
        
        // =================== SORT PHASE ===================
        // Sort this thread's intermediate vector
        std::sort(job_ctx->per_thread_intermediate_vectors[thread_id].begin(),
                  job_ctx->per_thread_intermediate_vectors[thread_id].end(),
                  [](const IntermediatePair& a, const IntermediatePair& b) {
                      return *(a.first) < *(b.first);
                  });
        
        // Wait for all threads to complete Map and Sort phases
        job_ctx->processing_barrier->barrier();
        
        // =================== SHUFFLE PHASE (Thread 0 only) ===================
        if (thread_id == 0) {
            // Count total intermediate pairs
            uint32_t total_intermediate_pairs = 0;
            for (const auto& vec : job_ctx->per_thread_intermediate_vectors) {
                total_intermediate_pairs += vec.size();
            }
            
            job_ctx->updateJobState(SHUFFLE_STAGE, 0, total_intermediate_pairs);
            
            // Create indices for each thread's vector
            std::vector<size_t> thread_indices(job_ctx->multiThreadLevel, 0);
            uint32_t shuffled_count = 0;
            
            while (true) {
                // Find the smallest key among all thread vectors
                K2* min_key = nullptr;
                bool found_any = false;
                
                for (int i = 0; i < job_ctx->multiThreadLevel; ++i) {
                    if (thread_indices[i] < job_ctx->per_thread_intermediate_vectors[i].size()) {
                        K2* current_key = job_ctx->per_thread_intermediate_vectors[i][thread_indices[i]].first;
                        if (!found_any || *(current_key) < *(min_key)) {
                            min_key = current_key;
                            found_any = true;
                        }
                    }
                }
                
                if (!found_any) break; // All vectors exhausted
                
                // Collect all pairs with the minimum key
                IntermediateVec current_key_vector;
                for (int i = 0; i < job_ctx->multiThreadLevel; ++i) {
                    while (thread_indices[i] < job_ctx->per_thread_intermediate_vectors[i].size()) {
                        K2* current_key = job_ctx->per_thread_intermediate_vectors[i][thread_indices[i]].first;
                        
                        // Check if keys are equal (neither a < b nor b < a)
                        if (!(*(current_key) < *(min_key)) && !(*(min_key) < *(current_key))) {
                            current_key_vector.push_back(
                                job_ctx->per_thread_intermediate_vectors[i][thread_indices[i]]
                            );
                            thread_indices[i]++;
                            shuffled_count++;
                        } else {
                            break;
                        }
                    }
                }
                
                // Add the vector to shuffled vectors
                job_ctx->shuffled_vectors.push_back(std::move(current_key_vector));
                
                // Update progress
                job_ctx->updateJobState(SHUFFLE_STAGE, shuffled_count, total_intermediate_pairs);
            }
            
            // Set up for reduce phase
            job_ctx->updateJobState(REDUCE_STAGE, 0, job_ctx->shuffled_vectors.size());
        }
        
        // Wait for shuffle to complete
        job_ctx->processing_barrier->barrier();
        
        // =================== REDUCE PHASE ===================
        uint32_t total_reduce_tasks = job_ctx->shuffled_vectors.size();
        
        while (true) {
            uint32_t current_index = job_ctx->next_reduce_index.fetch_add(1);
            
            if (current_index >= total_reduce_tasks) {
                break; // No more reduce tasks
            }
            
            // Process the reduce task
            job_ctx->client_ptr->reduce(&job_ctx->shuffled_vectors[current_index], tc);
            
            // Increment completed count and update progress
            uint32_t completed = job_ctx->reduce_completed_count.fetch_add(1) + 1;
            job_ctx->updateJobState(REDUCE_STAGE, completed, total_reduce_tasks);
        }
        
    } catch (const std::exception& e) {
        std::cerr << "system error: " << e.what() << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << "system error: unknown error in worker thread" << std::endl;
        exit(1);
    }
}

// Framework interface implementations
JobHandle startMapReduceJob(const MapReduceClient& client,
                           const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel) {
    try {
        // Handle edge case: empty input
        if (inputVec.empty()) {
            // Create a minimal job context that immediately completes
            auto job_ctx = std::make_unique<JobContext>(1, &client, &inputVec, &outputVec);
            job_ctx->updateJobState(REDUCE_STAGE, 0, 0);
            return static_cast<JobHandle>(job_ctx.release());
        }
        
        // Ensure we don't create more threads than input items
        int effective_threads = std::min(multiThreadLevel, static_cast<int>(inputVec.size()));
        
        // Create job context
        auto job_ctx = std::make_unique<JobContext>(effective_threads, &client, &inputVec, &outputVec);
        
        // Start worker threads
        for (int i = 0; i < effective_threads; ++i) {
            job_ctx->worker_threads.emplace_back(workerThreadFunction, &job_ctx->thread_contexts[i]);
        }
        
        return static_cast<JobHandle>(job_ctx.release());
        
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: memory allocation failed" << std::endl;
        exit(1);
    } catch (const std::exception& e) {
        std::cerr << "system error: " << e.what() << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << "system error: unknown error in startMapReduceJob" << std::endl;
        exit(1);
    }
}

void waitForJob(JobHandle job) {
    if (!job) return;
    
    JobContext* job_ctx = static_cast<JobContext*>(job);
    
    // Use atomic flag to ensure threads are joined only once
    bool expected = false;
    if (job_ctx->joined_flag.compare_exchange_strong(expected, true)) {
        try {
            for (auto& thread : job_ctx->worker_threads) {
                if (thread.joinable()) {
                    thread.join();
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "system error: " << e.what() << std::endl;
            exit(1);
        } catch (...) {
            std::cerr << "system error: unknown error in waitForJob" << std::endl;
            exit(1);
        }
    }
}

void getJobState(JobHandle job, JobState* state) {
    if (!job || !state) return;
    
    JobContext* job_ctx = static_cast<JobContext*>(job);
    
    try {
        job_ctx->getJobState(state);
    } catch (...) {
        // In case of any error, return a safe state
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0.0f;
    }
}

void closeJobHandle(JobHandle job) {
    if (!job) return;
    
    // Wait for job to finish before closing
    waitForJob(job);
    
    try {
        // Delete the job context
        delete static_cast<JobContext*>(job);
    } catch (...) {
        // Ignore exceptions in destructor
    }
}

void emit2(K2* key, V2* value, void* context) {
    if (!key || !value || !context) return;
    
    ThreadContext* thread_ctx = static_cast<ThreadContext*>(context);
    JobContext* job_ctx = thread_ctx->job_context;
    
    try {
        // Add to this thread's intermediate vector (no mutex needed - each thread has its own)
        job_ctx->per_thread_intermediate_vectors[thread_ctx->thread_id].push_back(
            std::make_pair(key, value)
        );
    } catch (const std::exception& e) {
        std::cerr << "system error: " << e.what() << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << "system error: unknown error in emit2" << std::endl;
        exit(1);
    }
}

void emit3(K3* key, V3* value, void* context) {
    if (!key || !value || !context) return;
    
    ThreadContext* thread_ctx = static_cast<ThreadContext*>(context);
    JobContext* job_ctx = thread_ctx->job_context;
    
    try {
        // Add to output vector with mutex protection
        std::lock_guard<std::mutex> lock(job_ctx->output_vector_mutex);
        job_ctx->output_vec_ptr->push_back(std::make_pair(key, value));
    } catch (const std::exception& e) {
        std::cerr << "system error: " << e.what() << std::endl;
        exit(1);
    } catch (...) {
        std::cerr << "system error: unknown error in emit3" << std::endl;
        exit(1);
    }
}