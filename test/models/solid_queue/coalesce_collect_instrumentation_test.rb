# frozen_string_literal: true

require "test_helper"

# What each event and monitoring query means, as the README states it
class SolidQueue::CoalesceCollectInstrumentationTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Rails 7.2 and main can defer enqueues to the commit, and these are about what happens without that
  class DiscardJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class CollectJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 3

    def perform_calls(_calls); end
  end

  class RecordsJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect

    def perform_calls(_calls); end
  end

  class FlakyJob < ApplicationJob
    coalesce key: "flaky", on_conflict: :collect
    retry_on ExpectedTestError, wait: 1.hour, attempts: 2

    def perform_calls(_calls)
      raise ExpectedTestError
    end
  end

  class FailingJob < ApplicationJob
    coalesce key: "failing", on_conflict: :collect

    def perform_calls(_calls)
      raise ExpectedTestError
    end
  end

  setup do
    @a = JobResult.create!(queue_name: "default", status: "a")
    @b = JobResult.create!(queue_name: "default", status: "b")
  end

  test "an admission's event comes after its commit" do
    open_transaction = nil
    subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") { open_transaction = SolidQueue::Record.connection_pool.with_connection(&:transaction_open?) }

    CollectJob.perform_later("g", 1)

    assert_equal false, open_transaction
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  test "inside an outer queue transaction the event comes before the commit, and stays fired after a rollback" do
    events = capture_events("coalesce.solid_queue") do
      SolidQueue::Record.transaction do
        CollectJob.perform_later("g", 1)
        raise ActiveRecord::Rollback
      end
    end

    assert_equal [ :created ], events.map { |event| event.payload[:status] }
    assert_equal 0, SolidQueue::Job.count
  end

  test "an app database transaction doesn't cover the queue's own commit" do
    events = capture_events("coalesce.solid_queue") do
      JobResult.transaction do
        CollectJob.perform_later("g", 1)
        raise ActiveRecord::Rollback
      end
    end

    assert_equal 1, events.size
    assert_equal 1, SolidQueue::Job.count
  end

  if SolidQueue::Record.respond_to?(:current_transaction)
    test "counting from after_commit counts what committed, once" do
      assert_equal [ :created ], counted_after_commit { CollectJob.perform_later("g", 1) }

      assert_equal [ :collected ], counted_after_commit {
        SolidQueue::Record.transaction { CollectJob.perform_later("g", 2) }
      }

      assert_equal [], counted_after_commit {
        SolidQueue::Record.transaction do
          CollectJob.perform_later("g", 3)
          raise ActiveRecord::Rollback
        end
      }
    end

    test "counting from after_commit drops what a savepoint rolled back" do
      counted = counted_after_commit do
        SolidQueue::Record.transaction do
          SolidQueue::Record.transaction(requires_new: true) do
            DiscardJob.perform_later("rolled-back")
            raise ActiveRecord::Rollback
          end

          DiscardJob.perform_later("kept")
          DiscardJob.perform_later("kept")
        end
      end

      assert_equal [ :created, :discarded ], counted
    end

    test "counting from after_commit ignores an app database transaction" do
      counted = counted_after_commit do
        JobResult.transaction do
          CollectJob.perform_later("g", 1)
          raise ActiveRecord::Rollback
        end
      end

      assert_equal [ :created ], counted
    end
  end

  test "a worker starting a job emits no coalesce event" do
    CollectJob.perform_later("g", 1)

    assert_empty capture_events("coalesce.solid_queue") { perform_ready_jobs }
  end

  test "a retry emits no coalesce event and keeps the active job id" do
    FlakyJob.perform_later(1)
    job = SolidQueue::Job.sole

    events = capture_events("coalesce.solid_queue") { perform_ready_jobs }

    assert_empty events
    assert_equal job.active_job_id, SolidQueue::Job.where(finished_at: nil).sole.active_job_id
  end

  test "a skipped call is reported without a deletion outcome when other calls run" do
    RecordsJob.perform_later(@a)
    RecordsJob.perform_later(@b)
    job = SolidQueue::Job.sole
    skipped_id = job.coalesced_calls.order(:id).first.id
    @a.destroy!

    payload = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }.sole.payload

    assert_equal skipped_id, payload[:call_id]
    assert_equal RecordsJob.name, payload[:class_name]
    assert_equal %i[ active_job_id call_id class_name exception_class job_id ], payload.keys.sort
  end

  test "every missing record is reported on each attempt" do
    RecordsJob.perform_later(@a)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    # The worker dies before recording the job as finished
    first_run = capture_events("drop_coalesced_call.solid_queue") { claimed.send(:execute) }
    assert_equal 1, job.coalesced_calls.count
    claimed.release
    re_run = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal 1, first_run.size
    assert_equal first_run.sole.payload[:call_id], re_run.sole.payload[:call_id]
    assert_not first_run.sole.payload.key?(:call_deleted)
    assert_not re_run.sole.payload.key?(:call_deleted)
  end

  test "skipping missing records doesn't write to the calls table" do
    RecordsJob.perform_later(@a)
    RecordsJob.perform_later(@b)
    job = SolidQueue::Job.sole
    rows = job.coalesced_calls.order(:id).pluck(:id, :arguments)
    @a.destroy!
    writes = []
    subscriber = ->(*, payload) do
      writes << payload[:sql] if payload[:sql].match?(/\A(?:DELETE FROM|UPDATE|INSERT INTO) ["`]?solid_queue_coalesced_calls\b/)
    end

    SolidQueue.with(preserve_finished_jobs: true) do
      ActiveSupport::Notifications.subscribed(subscriber, "sql.active_record") { perform_ready_jobs }
    end

    assert job.reload.finished?
    assert_equal rows, job.coalesced_calls.order(:id).pluck(:id, :arguments)
    assert_empty writes
  end

  test "perform.active_job reports a job's call count on each attempt" do
    CollectJob.perform_later("g", 1)
    CollectJob.perform_later("g", 2)
    DiscardJob.perform_later("g")

    sizes = call_counts { perform_ready_jobs }

    assert_equal [ 2 ], sizes
  end

  test "perform.active_job reports a size of 0 when every record is gone" do
    RecordsJob.perform_later(@a)
    @a.destroy!

    assert_equal [ 0 ], call_counts { perform_ready_jobs }
  end

  test "a job waiting to retry counts in waiting, with all of its calls" do
    FlakyJob.perform_later(1)
    FlakyJob.perform_later(2)

    perform_ready_jobs

    assert_equal 2, SolidQueue::CoalescedCall.waiting.count
  end

  test "the oldest waiting call's age, and nil when nothing is waiting" do
    assert_nil SolidQueue::CoalescedCall.waiting.minimum(:created_at)

    CollectJob.perform_later("g", 1)
    oldest = SolidQueue::CoalescedCall.sole.created_at
    travel 1.minute
    CollectJob.perform_later("g", 2)

    assert_equal oldest, SolidQueue::CoalescedCall.waiting.minimum(:created_at)
  ensure
    travel_back
  end

  test "failed collected jobs and the calls they hold are separate numbers" do
    FailingJob.perform_later(1)
    FailingJob.perform_later(2)
    DiscardJob.perform_later("failed-too")
    SolidQueue::Job.find_by!(class_name: DiscardJob.name).failed_with(ExpectedTestError.new("boom"))

    assert_raises(ExpectedTestError) { perform_ready_jobs }

    assert_equal 2, SolidQueue::FailedExecution.count
    assert_equal 1, SolidQueue::FailedExecution.where(job_id: SolidQueue::CoalescedCall.select(:job_id)).count
    assert_equal 2, SolidQueue::CoalescedCall.where(job_id: SolidQueue::FailedExecution.select(:job_id)).count
  end

  private
    def capture_events(name)
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(name) { |*args| events << ActiveSupport::Notifications::Event.new(*args) }
      yield
      events
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def counted_after_commit
      counted = []
      subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") do |*, payload|
        SolidQueue::Record.current_transaction.after_commit { counted << payload[:status] }
      end
      yield
      counted
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def call_counts
      sizes = []
      subscriber = ActiveSupport::Notifications.subscribe("perform.active_job") do |*, payload|
        calls = payload[:job].coalesced_calls
        sizes << calls.size if calls
      end
      yield
      sizes
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end
end
