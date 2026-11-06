# frozen_string_literal: true

require "test_helper"

class CoalescingTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  setup do
    close_connections_before_fork
    @pid = run_supervisor_as_fork(workers: [ { queues: "*", polling_interval: 0.01, processes: 2, threads: 2 } ], dispatchers: [ { polling_interval: 0.01, batch_size: 100 } ])
    wait_for_registered_processes(4, timeout: 5.seconds)
  end

  teardown do
    terminate_process(@pid) if process_exists?(@pid)
  end

  test "calls from several processes reach running workers and every job that runs finishes" do
    close_connections_before_fork
    pids = 4.times.map do
      fork do
        25.times { CoalescedResultJob.perform_later("g") }
        exit!(0)
      end
    end

    statuses = pids.map { |pid| Process.wait2(pid).last }
    assert statuses.all?(&:success?), "a writer process failed"

    wait_for(timeout: 15.seconds) do
      SolidQueue::CoalesceKey.none? && SolidQueue::Job.where(finished_at: nil).none?
    end

    finished = skip_active_record_query_cache { SolidQueue::Job.where(class_name: "CoalescedResultJob").count }
    runs = skip_active_record_query_cache { JobResult.where(status: "ran").count }

    assert_equal finished, runs
    assert_operator runs, :>=, 1
    assert_operator runs, :<=, 100
  end

  test "collected calls from several processes reach running workers exactly once and in bounded runs" do
    writers = 4
    calls_per_writer = 25

    close_connections_before_fork
    pids = writers.times.map do |writer|
      fork do
        calls_per_writer.times { |i| CollectedResultsJob.perform_later("g", "#{writer}-#{i}") }
        exit!(0)
      end
    end

    statuses = pids.map { |pid| Process.wait2(pid).last }
    assert statuses.all?(&:success?), "a writer process failed"

    wait_for(timeout: 15.seconds) do
      SolidQueue::CoalesceKey.none? && SolidQueue::Job.where(finished_at: nil).none?
    end

    runs = skip_active_record_query_cache { JobResult.where(status: "collected").pluck(:value).map { |value| JSON.parse(value) } }
    expected = writers.times.flat_map { |writer| calls_per_writer.times.map { |i| "#{writer}-#{i}" } }

    assert_equal expected.sort, runs.flatten.sort
    assert_operator runs.map(&:size).max, :<=, 5
    assert_equal 0, SolidQueue::CoalescedCall.waiting.count
  end

  private
    # With mysql2, a child that garbage collects a client it inherited while it has
    # its own connection open loses that connection, with no Rails or Solid Queue
    # involved. Closing first means there's nothing connected to inherit.
    def close_connections_before_fork
      ActiveRecord::Base.connection_handler.clear_all_connections!(:all)
    end
end
