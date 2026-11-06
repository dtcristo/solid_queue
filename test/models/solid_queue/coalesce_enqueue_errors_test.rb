# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceEnqueueErrorsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class OrdinaryJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)

    def perform(*); end
  end

  class DiscardJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :never : false if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(key) { key }

    def perform(*); end
  end

  class DatabaseKeyJob < DiscardJob
    coalesce key: ->(*) { raise ActiveRecord::StatementInvalid, "key lookup failed" }
  end

  class DatabasePayloadJob < DiscardJob
    def serialize
      raise ActiveRecord::StatementInvalid, "payload lookup failed"
    end
  end

  [ DatabaseKeyJob, DatabasePayloadJob ].each do |job_class|
    test "#{job_class.name.demodulize} preparation wraps only single enqueue errors" do
      single = assert_raises(SolidQueue::Job::EnqueueError) { job_class.perform_later("a") }
      assert_instance_of ActiveRecord::StatementInvalid, single.cause
      assert_raises(ActiveRecord::StatementInvalid) { ActiveJob.perform_all_later(job_class.new("a")) }
      assert_empty SolidQueue::Job.all
    end
  end

  [ DiscardJob ].each do |job_class|
    test "#{job_class.name.demodulize} bulk enqueue propagates a database error unchanged" do
      error = ActiveRecord::StatementInvalid.new("database failure")

      raised = with_admission_error(error) do
        assert_raises(ActiveRecord::StatementInvalid) { ActiveJob.perform_all_later(job_class.new("a")) }
      end

      assert_same error, raised
      assert_empty SolidQueue::Job.all
    end

    test "#{job_class.name.demodulize} single enqueue wraps a database error with its cause" do
      error = ActiveRecord::StatementInvalid.new("database failure")

      raised = with_admission_error(error) do
        assert_raises(SolidQueue::Job::EnqueueError) { job_class.perform_later("a") }
      end

      assert_same error, raised.cause
      assert_equal error.backtrace, raised.backtrace
      assert_empty SolidQueue::Job.all
    end

    [ false, true ].each do |bulk|
      test "#{job_class.name.demodulize} #{bulk ? 'bulk' : 'single'} enqueue leaves subscriber errors raw after committing" do
        job = job_class.new("a")
        error = ActiveRecord::StatementInvalid.new("subscriber failure")
        callback = ->(*) { raise error }

        raised = ActiveSupport::Notifications.subscribed(callback, "coalesce.solid_queue") do
          assert_raises(ActiveRecord::StatementInvalid) { bulk ? ActiveJob.perform_all_later(job) : job.enqueue }
        end

        assert_same error, raised
        assert_equal :created, job.coalesce_status
        assert_equal job.job_id, SolidQueue::Job.find(job.provider_job_id).active_job_id
      end

      [ false, true ].each do |outer_transaction|
        test "#{job_class.name.demodulize} #{bulk ? 'bulk' : 'single'} deadlock #{outer_transaction ? 'inside a transaction' : 'exhausting retries'} preserves error and commit boundaries" do
          before = OrdinaryJob.new("before") if bulk
          failing = job_class.new("a")
          expected_error = bulk ? ActiveRecord::Deadlocked : SolidQueue::Job::EnqueueError
          enqueue = -> { bulk ? ActiveJob.perform_all_later(before, failing) : failing.enqueue }

          attempts = deadlocking_on(/INSERT.*solid_queue_coalesce_keys/i) do
            raised = assert_raises(expected_error) do
              if outer_transaction
                SolidQueue::Record.transaction { enqueue.call }
              else
                enqueue.call
              end
            end
            assert_instance_of ActiveRecord::Deadlocked, raised.cause unless bulk
          end

          assert_equal outer_transaction ? 1 : 6, attempts
          assert_nil failing.provider_job_id
          assert_nil failing.coalesce_status
          expected_jobs = bulk && !outer_transaction ? [ before.job_id ] : []
          assert_equal expected_jobs, SolidQueue::Job.pluck(:active_job_id)
          assert_empty SolidQueue::CoalesceKey.all
        end
      end
    end

    test "#{job_class.name.demodulize} MySQL overlong key wraps only a single enqueue" do
      skip "MySQL enforces the key column's length" unless SolidQueue::Record.connection_pool.with_connection { |connection| connection.adapter_name == "Mysql2" }

      raised = assert_raises(SolidQueue::Job::EnqueueError) { job_class.perform_later("a" * 256) }
      assert_instance_of ActiveRecord::ValueTooLong, raised.cause
      assert_raises(ActiveRecord::ValueTooLong) { ActiveJob.perform_all_later(job_class.new("a" * 256)) }
      assert_empty SolidQueue::Job.all
    end
  end

  private
    def with_admission_error(error, &block)
      callback = ->(*, payload) { raise error if payload[:sql].match?(/INSERT.*solid_queue_coalesce_keys/i) }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
    end
end
