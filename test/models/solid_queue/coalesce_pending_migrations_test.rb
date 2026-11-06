# frozen_string_literal: true

require "test_helper"
require_relative "../../../lib/generators/solid_queue/update/templates/db/add_coalesce_keys_to_solid_queue"

class SolidQueue::CoalescePendingMigrationsTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Recreate an app that hasn't run the optional migration by reverting the
  # actual migration that ships with the update generator.
  setup do
    migrate(:down)
  end

  teardown do
    migrate(:up)
    destroy_records
  end

  test "the coalescing schema counts as pending migrations" do
    assert_not SolidQueue::CoalesceKey.migrated?
  end

  test "enqueueing a coalesced job raises a clear error, single or bulk" do
    error = assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { CoalescedRefreshJob.perform_later("a") }
    assert_match(/solid_queue:update/, error.message)

    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { SolidQueue::Job.enqueue_all([ CoalescedRefreshJob.new("a") ]) }
    assert_equal 0, SolidQueue::Job.count
  end

  test "enqueueing a collecting job raises the same error, single or bulk" do
    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { CollectedCallsJob.perform_later("g", 1) }
    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { SolidQueue::Job.enqueue_all([ CollectedCallsJob.new("g", 1) ]) }
    assert_equal 0, SolidQueue::Job.count
  end

  test "a keys table without the calls table still counts as pending" do
    SolidQueue::Record.connection_pool.with_connection do |connection|
      connection.create_table(:solid_queue_coalesce_keys) { |t| t.string :key, null: false }
    end
    SolidQueue::CoalesceKey.instance_variable_set(:@migrated, nil)

    assert_not SolidQueue::CoalesceKey.migrated?
    assert_raises(SolidQueue::CoalesceKey::PendingMigrations) { CoalescedRefreshJob.perform_later("a") }
  ensure
    SolidQueue::Record.connection_pool.with_connection { |connection| connection.drop_table(:solid_queue_coalesce_keys, if_exists: true) }
  end

  test "normal jobs enqueue, run, fail and retry as before" do
    active_job = AddToBufferJob.perform_later("hey")
    ActiveJob.perform_all_later([ AddToBufferJob.new("ho") ])
    assert_equal 2, SolidQueue::Job.where(class_name: "AddToBufferJob").count

    perform_ready_jobs
    assert_equal %w[ hey ho ], JobBuffer.values.sort

    SolidQueue::Job.find_by!(active_job_id: active_job.job_id).failed_with(ExpectedTestError.new("boom"))
    assert_equal 1, SolidQueue::FailedExecution.count
  end

  test "perform_now of a coalesced job doesn't need the table" do
    CoalescedRefreshJob.perform_now("a")

    assert_equal [ "a" ], JobBuffer.values
  end

  test "perform_now of a collecting job doesn't need the tables" do
    CollectedCallsJob.perform_now("g", 1)

    assert_equal [ [ [ "g", 1 ] ] ], JobBuffer.values
  end

  private
    def migrate(direction)
      ActiveRecord::Migration.suppress_messages do
        SolidQueue::Record.connection_pool.with_connection do |connection|
          AddCoalesceKeysToSolidQueue.new.exec_migration(connection, direction)
        end
      end

      SolidQueue::CoalesceKey.instance_variable_set(:@migrated, nil)
      SolidQueue::Record.connection_pool.disconnect!
    end
end
