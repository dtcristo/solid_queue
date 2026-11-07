module JobsTestHelper
  private

  # Claims and performs what's ready, the way a worker does, in this thread.
  def perform_ready_jobs
    process = SolidQueue::Process.find_by(name: "test-worker") || SolidQueue::Process.register(kind: "Worker", pid: 42, name: "test-worker")

    SolidQueue::ReadyExecution.claim("*", 100, process.id).each(&:perform)
  end

  # Calls the block in the middle of each job's run, after it has started.
  def perform_ready_jobs_with_callback(&block)
    results = []
    callback = ->(*) { results << block.call }

    ActiveSupport::Notifications.subscribed(callback, "perform_start.active_job") { perform_ready_jobs }
    results
  end

  # The name is in both places, and a rename in a deploy leaves both unresolvable.
  def rename_job_class(job, name)
    job.update!(class_name: name, arguments: job.arguments.merge("job_class" => name))
  end

  def stop_coalescing(job_class)
    job_class.coalesce_key = job_class.coalesce_on_conflict = job_class.coalesce_max_calls = nil
  end

  # What has to hold whenever nothing is in the middle of a transaction: a key's job
  # is marked, live and hasn't started, and its count is the number of calls that job has.
  def assert_coalesce_invariants
    SolidQueue::CoalesceKey.find_each do |coalesce_key|
      job = coalesce_key.job

      assert job, "#{coalesce_key.key} has no job"
      assert job.arguments.key?("solid_queue_coalesce_on_conflict"), "#{coalesce_key.key} is held by an unmarked job"
      assert_not job.finished?, "#{coalesce_key.key} is held by a finished job"
      assert_nil job.claimed_execution, "#{coalesce_key.key} is held by a job that started"
      assert_nil job.failed_execution, "#{coalesce_key.key} is held by a failed job"
      assert_equal coalesce_key.calls_count, job.coalesced_calls.count, "#{coalesce_key.key} has the wrong count"
    end

    assert_empty SolidQueue::CoalescedCall.where.missing(:job).pluck(:id), "calls without a job"
  end

  # Raises what a deadlock raises each time the database runs a matching statement,
  # after it ran. Returns how many times it did.
  def deadlocking_on(statement, &block)
    deadlocks = 0
    callback = ->(*, payload) do
      if payload[:sql].match?(statement)
        deadlocks += 1
        raise ActiveRecord::Deadlocked, "boom"
      end
    end

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
    deadlocks
  end

  def wait_for_jobs_to_finish_for(timeout = 1.second, except: [])
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Job.where.not(active_job_id: Array(except).map(&:job_id)).where(finished_at: nil).any?
      end
    end
  end

  def wait_for_jobs_to_be_released_for(timeout = 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::ClaimedExecution.count > 0
      end
    end
  end

  def wait_for_batches_to_finish_for(timeout = 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Batch.where(finished_at: nil).any?
      end
    end
  end

  def assert_unfinished_jobs(*jobs)
    skip_active_record_query_cache do
      assert_equal jobs.map(&:job_id).sort, SolidQueue::Job.where(finished_at: nil).map(&:active_job_id).sort
    end
  end

  def assert_no_unfinished_jobs
    skip_active_record_query_cache do
      assert SolidQueue::Job.where(finished_at: nil).none?
    end
  end

  def wait_for_jobs_to_be_enqueued(count, timeout: 1.second)
    wait_while_with_timeout(timeout) do
      skip_active_record_query_cache do
        SolidQueue::Job.count < count
      end
    end
  end

  def assert_no_claimed_jobs
    skip_active_record_query_cache do
      assert SolidQueue::ClaimedExecution.none?
    end
  end

  def assert_claimed_jobs(count = 1)
    skip_active_record_query_cache do
      assert_equal count, SolidQueue::ClaimedExecution.count
    end
  end
end
