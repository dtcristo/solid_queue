# frozen_string_literal: true

require "test_helper"
require "generators/solid_queue/update/templates/db/add_batches_to_solid_queue"

class SolidQueue::CoalesceWithoutBatchesTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  # Recreate an app that hasn't run Solid Queue's optional batches migration by
  # reverting the actual migration.
  setup do
    migrate(:down)
  end

  teardown do
    migrate(:up)
    destroy_records
  end

  test "the batches schema counts as missing" do
    assert_not SolidQueue::Batch.migrated?
  end

  test "coalesced jobs enqueue and run without it" do
    2.times { CoalescedRefreshJob.perform_later("a") }
    ActiveJob.perform_all_later(2.times.map { CoalescedRefreshJob.new("b") })

    assert_equal 2, SolidQueue::Job.count

    perform_ready_jobs

    assert_equal [ "a", "b" ], JobBuffer.values.sort
  end

  test "collecting jobs enqueue and run without it" do
    2.times { |i| CollectedCallsJob.perform_later("g", i) }

    assert_equal 1, SolidQueue::Job.count

    perform_ready_jobs

    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
  end

  private
    def migrate(direction)
      ActiveRecord::Migration.suppress_messages do
        SolidQueue::Record.connection_pool.with_connection do |connection|
          AddBatchesToSolidQueue.new.exec_migration(connection, direction)
        end
      end

      [ SolidQueue::Job, SolidQueue::Batch, SolidQueue::BatchExecution ].each(&:reset_column_information)
      SolidQueue::Batch.instance_variable_set(:@migrated, nil)
      SolidQueue::Record.connection_pool.disconnect!
    end
end
