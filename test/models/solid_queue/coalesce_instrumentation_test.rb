# frozen_string_literal: true

require "test_helper"

# What each event means, as the README states it
class SolidQueue::CoalesceInstrumentationTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Rails 7.2 and main can defer enqueues to the commit, and these are about what happens without that
  class DiscardJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }

    def perform(*); end
  end

  class FlakyJob < ApplicationJob
    coalesce key: "flaky"
    retry_on ExpectedTestError, wait: 1.hour, attempts: 2

    def perform(*)
      raise ExpectedTestError
    end
  end

  test "an admission's event comes after its commit" do
    open_transaction = nil
    subscriber = ActiveSupport::Notifications.subscribe("coalesce.solid_queue") { open_transaction = SolidQueue::Record.connection_pool.with_connection(&:transaction_open?) }

    DiscardJob.perform_later("g")

    assert_equal false, open_transaction
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber)
  end

  test "inside an outer queue transaction the event comes before the commit, and stays fired after a rollback" do
    events = capture_events("coalesce.solid_queue") do
      SolidQueue::Record.transaction do
        DiscardJob.perform_later("g")
        raise ActiveRecord::Rollback
      end
    end

    assert_equal [ :created ], events.map { |event| event.payload[:status] }
    assert_equal 0, SolidQueue::Job.count
  end

  test "an app database transaction doesn't cover the queue's own commit" do
    events = capture_events("coalesce.solid_queue") do
      JobResult.transaction do
        DiscardJob.perform_later("g")
        raise ActiveRecord::Rollback
      end
    end

    assert_equal 1, events.size
    assert_equal 1, SolidQueue::Job.count
  end

  if SolidQueue::Record.respond_to?(:current_transaction)
    test "counting from after_commit counts what committed, once" do
      assert_equal [ :created ], counted_after_commit { DiscardJob.perform_later("g") }

      assert_equal [ :discarded ], counted_after_commit {
        SolidQueue::Record.transaction { DiscardJob.perform_later("g") }
      }

      assert_equal [], counted_after_commit {
        SolidQueue::Record.transaction do
          DiscardJob.perform_later("h")
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
          DiscardJob.perform_later("g")
          raise ActiveRecord::Rollback
        end
      end

      assert_equal [ :created ], counted
    end
  end

  test "a discarded call's event names the call and the job it joined" do
    first = DiscardJob.perform_later("g")
    second = nil

    events = capture_events("coalesce.solid_queue") { second = DiscardJob.perform_later("g") }

    assert_equal({ status: :discarded, key: "#{DiscardJob.name}/g", job_id: first.provider_job_id, active_job_id: second.job_id, class_name: DiscardJob.name }, events.sole.payload)
  end

  test "a worker starting a job emits no coalesce event" do
    DiscardJob.perform_later("g")

    assert_empty capture_events("coalesce.solid_queue") { perform_ready_jobs }
  end

  test "a retry emits no coalesce event and keeps the active job id" do
    FlakyJob.perform_later(1)
    job = SolidQueue::Job.sole

    events = capture_events("coalesce.solid_queue") { perform_ready_jobs }

    assert_empty events
    assert_equal job.active_job_id, SolidQueue::Job.where(finished_at: nil).sole.active_job_id
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
end
