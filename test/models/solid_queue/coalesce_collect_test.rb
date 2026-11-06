# frozen_string_literal: true

require "test_helper"

class SolidQueue::CoalesceCollectTest < ActiveSupport::TestCase
  self.use_transactional_tests = false

  class Money
    attr_reader :cents

    def initialize(cents)
      @cents = cents
    end
  end

  class MoneySerializer < ActiveJob::Serializers::ObjectSerializer
    def serialize(money)
      super("cents" => money.cents)
    end

    def deserialize(hash)
      Money.new(hash["cents"])
    end

    def klass
      Money
    end
  end

  ActiveJob::Serializers.add_serializers MoneySerializer

  class RecordsCollectedJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class LimitedCallsJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2
    limits_concurrency key: ->(group, *) { group }, to: 1

    def perform_calls(calls)
      JobBuffer.add(calls.size)
    end
  end

  class DeferredCollectedJob < ApplicationJob
    self.enqueue_after_transaction_commit = ActiveJob.version < "8.0" ? :always : true if respond_to?(:enqueue_after_transaction_commit=)
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class InlineCollectedJob < ApplicationJob
    self.queue_adapter = :inline

    coalesce key: "inline", on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls)
    end
  end

  class TwoCallsJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class CountingSerializesJob < TwoCallsJob
    def serialize
      super.merge("serialize_count" => (@serialize_count = @serialize_count.to_i + 1))
    end
  end

  class OneAtATimeJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 1

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class TwoRecordsJob < ApplicationJob
    coalesce key: "records", on_conflict: :collect, max_calls: 2

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class DefaultMaxCallsJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls.map { |call| call.arguments.last })
    end
  end

  class SubclassedCollectedJob < CollectedCallsJob
    def perform_calls(calls)
      JobBuffer.add([ :subclass, calls.map(&:arguments) ])
    end
  end

  class ConcretePerformJob < ApplicationJob
    coalesce key: "concrete", on_conflict: :collect

    def perform(*)
      JobBuffer.add(:perform)
    end

    def perform_calls(calls)
      JobBuffer.add(:perform_calls)
    end
  end

  class NoPerformCallsJob < ApplicationJob
    coalesce key: "missing", on_conflict: :collect

    def perform(*)
      JobBuffer.add(:perform)
    end
  end

  class ExposingCallbacksJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 5

    before_perform { JobBuffer.add([ :before, arguments, coalesced_calls.map(&:arguments) ]) }
    around_perform do |job, block|
      JobBuffer.add([ :around, job.coalesced_calls.size ])
      block.call
    end
    after_perform { JobBuffer.add([ :after, coalesced_calls.size ]) }

    def perform_calls(calls)
      JobBuffer.add([ :perform_calls, arguments, calls.equal?(coalesced_calls) ])
    end
  end

  class LimitedCollectedJob < ApplicationJob
    coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 2
    limits_concurrency key: ->(_group, concurrency_group) { concurrency_group }

    def perform_calls(calls)
      JobBuffer.add(calls.map(&:arguments))
    end
  end

  class TenantJob < ApplicationJob
    coalesce key: "tenant", on_conflict: :collect

    cattr_accessor :current_tenant

    def serialize
      super.merge("tenant" => self.class.current_tenant)
    end

    def perform_calls(calls)
      JobBuffer.add(calls)
    end
  end

  class BulkIndexJob < ApplicationJob
    coalesce key: ->(status, *) { status }, on_conflict: :collect, max_calls: 100

    def perform_calls(calls)
      status = calls.first.arguments.first
      ids = calls.map { |call| call.arguments.last }.uniq

      JobBuffer.add(JobResult.where(status: status, id: ids).to_a)
    end
  end

  class WrappedPayloadJob < ApplicationJob
    coalesce key: "wrapped", on_conflict: :collect

    def serialize
      payload = super
      payload.merge("arguments" => [ "wrapped", payload["arguments"] ])
    end

    def deserialize(job_data)
      raise ArgumentError, "corrupt payload" if job_data["corrupt"]

      super(job_data.merge("arguments" => job_data["arguments"].last))
    end

    def perform_calls(calls)
      JobBuffer.add(calls)
    end
  end

  class ForwardingJob < ApplicationJob
    coalesce key: "forwarding", on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls.map { |call| handle(*call.arguments) })
    end

    private
      def handle(name, options = nil, force: false)
        [ name, options, force ]
      end
  end

  class MutatingDeserializeJob < ApplicationJob
    coalesce key: "mutating", on_conflict: :collect

    def serialize
      super.merge("field" => "kept")
    end

    def deserialize(job_data)
      super
      job_data.delete("field")
    end

    def perform_calls(calls)
      JobBuffer.add(calls)
    end
  end

  class ReversedArgumentsJob < ApplicationJob
    coalesce key: "reversed", on_conflict: :collect

    def perform_calls(calls)
      JobBuffer.add(calls)
    end

    private
      def serialize_arguments(arguments)
        arguments.map(&:reverse)
      end

      def deserialize_arguments(serialized_arguments)
        serialized_arguments.map(&:reverse)
      end
  end

  setup do
    @a, @b = JobResult.create!, JobResult.create!
  end

  test "every call stores its whole payload, including the first, and perform_calls gets them once in row order" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    second = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 2)

    assert first.successfully_enqueued?
    assert_equal :created, first.coalesce_status
    assert second.successfully_enqueued?
    assert_equal :collected, second.coalesce_status
    assert_equal first.provider_job_id, second.provider_job_id

    assert_equal 1, SolidQueue::Job.count
    rows = SolidQueue::CoalescedCall.waiting.order(:id).to_a
    assert_equal [ first.job_id, second.job_id ], rows.map { |row| row.arguments["job_id"] }
    assert_equal [ [ "g", 1 ], [ "g", 2 ] ], rows.map { |row| row.arguments["arguments"] }

    travel_to 2.hours.from_now do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end

    assert_equal [ [ [ "g", 1 ], [ "g", 2 ] ] ], JobBuffer.values
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_empty SolidQueue::CoalescedCall.waiting
    assert_equal rows.map(&:id), SolidQueue::CoalescedCall.where(job_id: first.provider_job_id).order(:id).pluck(:id)
  end

  test "a collected call reports its outcome with a status and an event" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    second = nil

    events = capture_events("coalesce.solid_queue") { second = CollectedCallsJob.perform_later("g", 2) }

    assert_equal :collected, second.coalesce_status
    assert_equal({ status: :collected, key: "CollectedCallsJob/g", job_id: first.provider_job_id, active_job_id: second.job_id, class_name: "CollectedCallsJob" }, events.sole.payload)
  end

  test "the calls table has only the columns it needs, and the keys table counts from 0" do
    assert_equal %w[ arguments created_at id job_id ], SolidQueue::CoalescedCall.column_names.sort
    assert_not SolidQueue::CoalescedCall.columns_hash["job_id"].null
    assert_equal "0", SolidQueue::CoalesceKey.columns_hash["calls_count"].default.to_s
    assert_not SolidQueue::CoalesceKey.columns_hash["calls_count"].null
  end

  test "job_data is the raw payload of each call, custom fields included, and read-only" do
    TenantJob.current_tenant = "first"
    first = TenantJob.set(wait: 1.hour, queue: "tenants", priority: 3).perform_later("a")
    TenantJob.current_tenant = "second"
    second = TenantJob.set(wait: 2.hours).perform_later("b")

    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.minute.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs

    calls = JobBuffer.values.sole
    assert_equal SolidQueue::CoalescedCall.order(:id).pluck(:id), calls.map(&:id)

    assert_equal first.job_id, calls.first.job_data["job_id"]
    assert_equal TenantJob.name, calls.first.job_data["job_class"]
    assert_equal "tenants", calls.first.job_data["queue_name"]
    assert_equal 3, calls.first.job_data["priority"]
    assert_equal [ "a" ], calls.first.job_data["arguments"]
    assert_equal 0, calls.first.job_data["executions"]
    assert_equal "en", calls.first.job_data["locale"]
    assert_equal "first", calls.first.job_data["tenant"]
    assert_nil calls.first.job_data["provider_job_id"], "the snapshot comes from before the job row existed"
    assert_in_delta first.scheduled_at.to_f, Time.iso8601(calls.first.job_data["scheduled_at"]).to_f, 0.001
    assert calls.first.job_data["enqueued_at"].present?
    assert calls.first.job_data.key?("timezone")

    assert_equal second.job_id, calls.last.job_data["job_id"]
    assert_equal "second", calls.last.job_data["tenant"]
    assert_in_delta second.scheduled_at.to_f, Time.iso8601(calls.last.job_data["scheduled_at"]).to_f, 0.001
    assert_equal calls.first.job_data.keys.sort, calls.last.job_data.keys.sort

    assert calls.first.job_data.frozen?
    assert_raises(FrozenError) { calls.first.job_data["job_id"] = "x" }
  ensure
    TenantJob.current_tenant = nil
  end

  test "a call exposes id, arguments and job_data, and nothing else" do
    assert_equal %i[ arguments id job_data ], ActiveJob::Coalescing::Call.public_instance_methods(false).sort
  end

  test "call arguments come back as the positional array, with records, serializers and the keyword marker" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@a, 1)
    RecordsCollectedJob.perform_later(@b, force: true)
    RecordsCollectedJob.perform_later(only: "keywords")
    RecordsCollectedJob.perform_later
    RecordsCollectedJob.perform_later(nil)
    RecordsCollectedJob.perform_later(Money.new(250))

    perform_ready_jobs

    arguments = JobBuffer.values.sole
    assert_equal [ [ @a ], [ @a, 1 ], [ @b, { force: true } ], [ { only: "keywords" } ], [], [ nil ] ], arguments.first(6)

    assert Hash.ruby2_keywords_hash?(arguments[2].last)
    assert Hash.ruby2_keywords_hash?(arguments[3].last)
    assert_equal({ force: true }, ->(_record, force:) { { force: force } }.call(*arguments[2]))

    assert_kind_of Money, arguments[6].first
    assert_equal 250, arguments[6].first.cents
  end

  test "a trailing hash that wasn't keywords stays a plain hash" do
    RecordsCollectedJob.perform_later(@a, { "force" => true })

    perform_ready_jobs

    assert_not Hash.ruby2_keywords_hash?(JobBuffer.values.sole.first.last)
  end

  test "call arguments go through the job's own serialize and deserialize, and job_data stays as stored" do
    WrappedPayloadJob.perform_later("x")
    WrappedPayloadJob.perform_later("y", force: true)

    perform_ready_jobs

    calls = JobBuffer.values.sole
    assert_equal [ [ "x" ], [ "y", { force: true } ] ], calls.map(&:arguments)
    assert Hash.ruby2_keywords_hash?(calls.last.arguments.last)
    assert_equal [ "wrapped", [ "x" ] ], calls.first.job_data["arguments"]
  end

  test "call arguments go through the job's own argument serialization, and job_data stays as stored" do
    ReversedArgumentsJob.perform_later("abc")
    ReversedArgumentsJob.perform_later("xyz")

    perform_ready_jobs

    calls = JobBuffer.values.sole
    assert_equal [ [ "abc" ], [ "xyz" ] ], calls.map(&:arguments)
    assert_equal [ "cba" ], calls.first.job_data["arguments"]
  end

  test "a deserialize override that changes its argument leaves job_data as stored" do
    MutatingDeserializeJob.perform_later("x")

    perform_ready_jobs

    assert_equal "kept", JobBuffer.values.sole.sole.job_data["field"]
  end

  test "a job with its own deserialize skips the call whose record is gone, and runs the others" do
    gone = JobResult.create!
    WrappedPayloadJob.perform_later(@a)
    WrappedPayloadJob.perform_later(gone)
    WrappedPayloadJob.perform_later(@b)
    gone.destroy!

    events = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ [ @a ], [ @b ] ], JobBuffer.values.sole.map(&:arguments)
    assert_equal 3, SolidQueue::CoalescedCall.count
    assert_equal 1, events.size
  end

  test "an error from a job's own deserialize fails the run and keeps the calls" do
    WrappedPayloadJob.perform_later(@a)
    WrappedPayloadJob.perform_later("y")
    job = SolidQueue::Job.sole
    corrupt = SolidQueue::CoalescedCall.order(:id).last
    corrupt.update!(arguments: corrupt.arguments.merge("corrupt" => true))
    @a.destroy!

    events = capture_events("drop_coalesced_call.solid_queue") do
      assert_raises(ArgumentError) { perform_ready_jobs }
    end

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
    assert_empty JobBuffer.values
    assert_empty events
  end

  test "forwarding a call's arguments keeps keywords and trailing hashes apart" do
    ForwardingJob.perform_later("a", force: true)
    ForwardingJob.perform_later("a", { force: true })
    ForwardingJob.perform_later("a", { "force" => true })

    perform_ready_jobs

    assert_equal [ [ "a", nil, true ], [ "a", { force: true }, false ], [ "a", { "force" => true }, false ] ], JobBuffer.values.sole
  end

  test "ids from a job's calls feed one bounded query" do
    records = 3.times.map { JobResult.create!(status: "account") }
    records.each { |record| BulkIndexJob.perform_later("account", record.id) }
    BulkIndexJob.perform_later("account", records.first.id)
    queries = []
    callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].match?(/\ASELECT .*job_results/) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { perform_ready_jobs }

    assert_equal records.sort_by(&:id), JobBuffer.values.sole.sort_by(&:id)
    assert_equal 1, queries.size
  end

  test "perform_calls in a subclass gets the calls too" do
    SubclassedCollectedJob.perform_later("g", 1)

    perform_ready_jobs

    assert_equal [ [ :subclass, [ [ "g", 1 ] ] ] ], JobBuffer.values
  end

  test "a collecting job runs perform_calls, even when it has a perform" do
    ConcretePerformJob.perform_later("a")

    perform_ready_jobs

    assert_equal [ :perform_calls ], JobBuffer.values
  end

  test "a collecting job without perform_calls fails its run, keeps its calls and says what to do" do
    NoPerformCallsJob.perform_later("a")
    job = SolidQueue::Job.sole

    error = assert_raises(NotImplementedError) { perform_ready_jobs }

    assert_match(/#{NoPerformCallsJob.name}.*perform_calls/, error.message)
    assert_empty JobBuffer.values
    assert job.reload.failed?
    assert_equal 1, job.coalesced_calls.count
  end

  test "perform_now of a collecting job without perform_calls raises too" do
    assert_raises(NotImplementedError) { NoPerformCallsJob.perform_now("a") }
  end

  test "perform callbacks see coalesced_calls, and arguments stay those of the job's first call" do
    ExposingCallbacksJob.perform_later("g", 1)
    ExposingCallbacksJob.perform_later("g", 2)

    perform_ready_jobs

    assert_equal [
      [ :before, [ "g", 1 ], [ [ "g", 1 ], [ "g", 2 ] ] ],
      [ :around, 2 ],
      [ :perform_calls, [ "g", 1 ], true ],
      [ :after, 2 ]
    ], JobBuffer.values
  end

  test "calls fill one job at a time: full jobs are sealed, at most one stays open, and no job is empty" do
    { 1 => [ 1 ], 99 => [ 99 ], 100 => [ 100 ], 101 => [ 100, 1 ], 250 => [ 100, 100, 50 ], 350 => [ 100, 100, 100, 50 ] }.each do |count, sizes|
      group = "group#{count}"
      active_jobs = count.times.map { |i| DefaultMaxCallsJob.perform_later(group, i) }

      jobs = active_jobs.map(&:provider_job_id).uniq
      assert_equal sizes, jobs.map { |job_id| SolidQueue::CoalescedCall.where(job_id: job_id).count }, group

      open_key = SolidQueue::CoalesceKey.find_by(key: "#{DefaultMaxCallsJob.name}/#{group}")
      if sizes.last < 100
        assert_equal jobs.last, open_key.job_id, group
        assert_equal sizes.last, open_key.calls_count, group
      else
        assert_nil open_key, group
      end
    end
  end

  test "backlogs of 1, 100 and 101 run in jobs of the default size" do
    { "one" => 1, "hundred" => 100, "hundred_and_one" => 101 }.each do |group, count|
      JobBuffer.clear
      count.times { |i| DefaultMaxCallsJob.perform_later(group, i) }

      perform_ready_jobs
      perform_ready_jobs

      assert_equal (0...count).each_slice(100).to_a, JobBuffer.values, group
      assert_equal 0, SolidQueue::CoalesceKey.count
    end
  end

  test "a start takes the calls that were assigned to its job, however big the backlog" do
    7.times { |i| TwoCallsJob.set(wait: 1.hour).perform_later("g", i) }
    SolidQueue::ScheduledExecution.update_all(scheduled_at: 1.minute.ago)
    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    job = SolidQueue::Job.order(:id).first
    oldest = SolidQueue::CoalescedCall.order(:id).limit(2).pluck(:id)
    SolidQueue::ReadyExecution.where.not(job_id: job.id).delete_all

    perform_ready_jobs

    assert_equal oldest, job.coalesced_calls.order(:id).pluck(:id)
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
  end

  test "max_calls 1 gives every call its own sealed job" do
    active_jobs = 3.times.map { |i| OneAtATimeJob.perform_later("g", i) }

    assert_equal 3, active_jobs.map(&:provider_job_id).uniq.size
    assert_equal [ :created ] * 3, active_jobs.map(&:coalesce_status)
    assert_equal 0, SolidQueue::CoalesceKey.count

    perform_ready_jobs

    assert_equal [ [ [ "g", 0 ] ], [ [ "g", 1 ] ], [ [ "g", 2 ] ] ], JobBuffer.values
  end

  test "an open job over a lowered max_calls is sealed as it is, and the calls go to a new job" do
    first = TwoCallsJob.perform_later("g", 0)
    SolidQueue::CoalesceKey.sole.update!(calls_count: 5)

    second = TwoCallsJob.perform_later("g", 1)

    assert_equal :created, second.coalesce_status
    assert_not_equal first.provider_job_id, second.provider_job_id
    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
    assert_equal second.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "a large backlog drains in order, in bounded jobs" do
    7.times { |i| TwoCallsJob.perform_later("g", i) }

    4.times { perform_ready_jobs }

    assert_equal [ 2, 2, 2, 1 ], JobBuffer.values.map(&:size)
    assert_equal (0..6).map { |i| [ "g", i ] }, JobBuffer.values.flatten(1)
    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
  end

  test "each job takes its schedule, queue and priority from its own first call" do
    in_an_hour = 1.hour.from_now
    in_two_hours = 2.hours.from_now

    calls = [
      TwoCallsJob.set(queue: "first", priority: 1).perform_later("g", 0),
      TwoCallsJob.set(wait_until: in_two_hours, queue: "ignored", priority: 9).perform_later("g", 1),
      TwoCallsJob.set(wait_until: in_an_hour, queue: "second", priority: 2).perform_later("g", 2),
      TwoCallsJob.set(wait_until: in_two_hours, queue: "ignored", priority: 9).perform_later("g", 3),
      TwoCallsJob.set(wait_until: in_two_hours, queue: "third", priority: 3).perform_later("g", 4)
    ]

    first, second, third = SolidQueue::Job.order(:id).to_a
    assert_equal [ calls[0], calls[2], calls[4] ].map(&:job_id), [ first, second, third ].map(&:active_job_id)
    assert_equal %w[ first second third ], [ first, second, third ].map(&:queue_name)
    assert_equal [ 1, 2, 3 ], [ first, second, third ].map(&:priority)
    assert first.ready?
    assert_in_delta in_an_hour.to_f, second.scheduled_at.to_f, 0.001
    assert_in_delta in_two_hours.to_f, third.scheduled_at.to_f, 0.001

    assert_equal [ "g", 2 ], second.arguments["arguments"]
    assert_equal third.id, SolidQueue::CoalesceKey.sole.job_id
  end

  test "a full delayed job waits for its time, and a later job with no wait can overtake it" do
    in_an_hour = 1.hour.from_now

    2.times { |i| TwoCallsJob.set(wait_until: in_an_hour).perform_later("g", i) }
    delayed = SolidQueue::Job.sole
    TwoCallsJob.perform_later("g", 2)

    assert delayed.scheduled?
    assert_not delayed.ready?

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs
    assert_equal [ [ [ "g", 2 ] ] ], JobBuffer.values

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)
      perform_ready_jobs
    end
    assert_equal [ [ "g", 0 ], [ "g", 1 ] ], JobBuffer.last_value
  end

  test "sealed jobs run independently, and can overlap" do
    4.times { |i| TwoCallsJob.perform_later("g", i) }
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "overlap-worker")
    first, second = SolidQueue::ReadyExecution.claim("*", 2, process.id).sort_by(&:job_id)

    overlapping = [ second ]
    callback = ->(*) { overlapping.shift&.perform }

    ActiveSupport::Notifications.subscribed(callback, "perform_start.active_job") { first.perform }

    assert_equal [ [ [ "g", 2 ], [ "g", 3 ] ], [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
  end

  test "every call in a job runs with its own payload" do
    active_jobs = 5.times.map { |i| TwoRecordsJob.set(priority: i).perform_later(i) }
    calls = []
    callback = ->(*, payload) { calls.concat(payload[:job].coalesced_calls) }

    ActiveSupport::Notifications.subscribed(callback, "perform.active_job") { perform_ready_jobs }

    assert_equal active_jobs.map(&:job_id), calls.map { |call| call.job_data["job_id"] }
    assert_equal (0...5).to_a, calls.map { |call| call.job_data["priority"] }
    assert_equal (0...5).map { |i| [ i ] }, calls.map(&:arguments)
  end

  test "a bulk enqueue sets each job's schedule from its own first call" do
    in_an_hour = 1.hour.from_now
    jobs = [ TwoCallsJob.new("g", 0), TwoCallsJob.new("g", 1), TwoCallsJob.new("g", 2).set(wait_until: in_an_hour) ]

    SolidQueue::Job.enqueue_all(jobs)

    first, second = SolidQueue::Job.order(:id).to_a
    assert first.ready?
    assert second.scheduled?
    assert_in_delta in_an_hour.to_f, second.scheduled_at.to_f, 0.001
  end

  test "a bulk enqueue stores the schedule of an immediate call in its payload" do
    SolidQueue::Job.enqueue_all([ TwoCallsJob.new("g", 0) ])

    assert SolidQueue::CoalescedCall.sole.arguments["scheduled_at"].present?
  end

  test "each job's concurrency key comes from its own first call" do
    LimitedCollectedJob.perform_later("g", "first_group")
    LimitedCollectedJob.perform_later("g", "second_group")
    LimitedCollectedJob.perform_later("g", "third_group")

    first, second = SolidQueue::Job.order(:id).to_a
    assert_equal "#{LimitedCollectedJob.name}/first_group", first.concurrency_key
    assert_equal "#{LimitedCollectedJob.name}/third_group", second.concurrency_key
    assert_equal [ "g", "third_group" ], second.arguments["arguments"]
  end

  test "reaching max_calls never dispatches a scheduled job" do
    in_an_hour = 1.hour.from_now

    5.times { |i| TwoCallsJob.set(wait_until: in_an_hour).perform_later("g", i) }

    assert_equal 3, SolidQueue::Job.count
    assert SolidQueue::Job.all.all?(&:scheduled?)
    assert_equal 5, SolidQueue::CoalescedCall.waiting.count

    SolidQueue::ScheduledExecution.dispatch_next_batch(10)
    perform_ready_jobs
    assert_empty JobBuffer.values
  end

  test "a job that fills keeps its time" do
    2.times { |i| TwoCallsJob.set(wait: 1.hour).perform_later("g", i) }

    job = SolidQueue::Job.sole
    assert job.scheduled?
    assert_operator job.scheduled_at, :>, 50.minutes.from_now
  end

  test "a job that fills with its first call still waits for its time" do
    OneAtATimeJob.set(wait: 1.hour).perform_later("g", 1)

    assert SolidQueue::Job.sole.scheduled?
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a bulk enqueue past max_calls doesn't dispatch a scheduled job either" do
    jobs = 5.times.map { |i| TwoCallsJob.new("g", i).set(wait: 1.hour) }

    SolidQueue::Job.enqueue_all(jobs)

    assert_equal 3, SolidQueue::Job.count
    assert SolidQueue::Job.all.all?(&:scheduled?)
  end

  test "the first pending schedule wins, and a call that joins runs with that job even if it asked for later" do
    in_an_hour = 1.hour.from_now
    in_three_hours = 3.hours.from_now

    DefaultMaxCallsJob.set(wait_until: in_an_hour).perform_later("g", 0)
    job = SolidQueue::Job.sole
    assert_equal :collected, DefaultMaxCallsJob.set(wait_until: in_three_hours).perform_later("g", 1).coalesce_status
    assert_in_delta in_an_hour.to_f, job.reload.scheduled_at.to_f, 0.001

    travel_to in_an_hour + 1.second do
      SolidQueue::ScheduledExecution.dispatch_next_batch(10)

      after_start = perform_ready_jobs_with_callback do
        DefaultMaxCallsJob.set(wait_until: in_three_hours).perform_later("g", 2)
      end

      assert_equal :created, after_start.sole.coalesce_status
    end
    assert_equal [ [ 0, 1 ] ], JobBuffer.values

    assert_in_delta in_three_hours.to_f, SolidQueue::Job.where(finished_at: nil).sole.scheduled_at.to_f, 0.001
  end

  test "a blocked job stays blocked when calls join it" do
    LimitedCollectedJob.perform_later("holder", "shared")
    blocked = SolidQueue::Job.find(LimitedCollectedJob.perform_later("g", "shared").provider_job_id)
    assert blocked.blocked?

    LimitedCollectedJob.perform_later("g", "other")

    assert SolidQueue::Job.find(blocked.id).blocked?
    assert_equal 2, blocked.coalesced_calls.count
    assert_equal 2, SolidQueue::CoalescedCall.waiting.where(job_id: blocked.id).count
  end

  test "waiting is what hasn't started: pending, delayed and blocked jobs' calls, not those of running, failed or finished ones" do
    semaphore_holder = LimitedCollectedJob.perform_later("running", "shared")
    blocked = LimitedCollectedJob.perform_later("blocked", "shared")
    delayed = TwoCallsJob.set(wait: 1.hour).perform_later("delayed", 0)
    ready = TwoCallsJob.perform_later("ready", 0)
    SolidQueue::ReadyExecution.where.not(job_id: ready.provider_job_id).update_all(priority: 1)
    waiting = -> { SolidQueue::CoalescedCall.waiting.distinct.pluck(:job_id).sort }
    assert_equal [ semaphore_holder.provider_job_id, blocked.provider_job_id, delayed.provider_job_id, ready.provider_job_id ].sort, waiting.call

    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "waiting-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 1, process.id).sole
    assert_equal ready.provider_job_id, claimed.job_id
    assert_not_includes waiting.call, ready.provider_job_id

    claimed.perform
    assert_not_includes waiting.call, ready.provider_job_id
    assert SolidQueue::Job.find(ready.provider_job_id).finished?

    SolidQueue::Job.find(delayed.provider_job_id).failed_with(ExpectedTestError.new("boom"))
    assert_not_includes waiting.call, delayed.provider_job_id
  end

  test "calls during execution go to a new job and never join the running job" do
    CollectedCallsJob.perform_later("g", 0)
    CollectedCallsJob.perform_later("g", 1)

    joined = perform_ready_jobs_with_callback { CollectedCallsJob.perform_later("g", 2).coalesce_status }

    assert_equal [ :created ], joined
    assert_equal [ [ [ "g", 0 ], [ "g", 1 ] ] ], JobBuffer.values
    assert_equal 1, SolidQueue::CoalescedCall.waiting.count
    assert_equal 1, SolidQueue::ReadyExecution.count

    perform_ready_jobs
    assert_equal [ [ "g", 2 ] ], JobBuffer.last_value
  end

  test "bulk enqueue stores every call and counts them all" do
    jobs = 4.times.map { |i| CollectedCallsJob.new("g", i).set(wait: 1.hour) } + [ CollectedCallsJob.new("h", 9).set(wait: 1.hour) ]

    assert_difference -> { SolidQueue::Job.count }, +3 do
      assert_equal 5, SolidQueue::Job.enqueue_all(jobs)
    end

    assert jobs.all?(&:successfully_enqueued?)
    assert_equal [ :created, :collected, :collected, :created, :created ], jobs.map(&:coalesce_status)
    assert_equal jobs.map(&:job_id).sort, SolidQueue::CoalescedCall.waiting.map { |row| row.arguments["job_id"] }.sort
  end

  test "bulk enqueue over several job boundaries reports each call's own outcome and job, in call order" do
    open_job = TwoCallsJob.perform_later("g", 0).provider_job_id
    jobs = 6.times.map { |i| TwoCallsJob.new("g", i + 1) }

    events = capture_events("coalesce.solid_queue") { assert_equal 6, SolidQueue::Job.enqueue_all(jobs) }

    assert_equal [ :collected, :created, :collected, :created, :collected, :created ], jobs.map(&:coalesce_status)
    assert_equal open_job, jobs.first.provider_job_id
    jobs.each do |active_job|
      assert_includes SolidQueue::CoalescedCall.where(job_id: active_job.provider_job_id).map { |row| row.arguments["job_id"] }, active_job.job_id
    end
    assert_equal [ jobs[1], jobs[3], jobs[5] ].map(&:job_id), SolidQueue::Job.where.not(id: open_job).order(:id).pluck(:active_job_id)
    assert_equal jobs.map(&:coalesce_status), events.map { |event| event.payload[:status] }
    assert_equal jobs.map(&:provider_job_id), events.map { |event| event.payload[:job_id] }

    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
    assert_equal jobs.last.provider_job_id, SolidQueue::CoalesceKey.sole.job_id
    assert_equal (0..6).map { |i| [ "g", i ] }, SolidQueue::CoalescedCall.order(:id).map { |row| row.arguments["arguments"] }
  end

  test "250 calls in bulk make 3 jobs" do
    jobs = 250.times.map { |i| DefaultMaxCallsJob.new("g", i) }

    assert_equal 250, SolidQueue::Job.enqueue_all(jobs)

    assert_equal [ 100, 100, 50 ], jobs.map(&:provider_job_id).uniq.map { |id| SolidQueue::CoalescedCall.where(job_id: id).count }
    assert_equal 3, jobs.count { |active_job| active_job.coalesce_status == :created }
  end

  test "a deadlock retry in a bulk enqueue leaves correct outcomes, and no job from the attempt that failed" do
    open_job = CountingSerializesJob.perform_later("g", 0).provider_job_id
    jobs = 3.times.map { |i| CountingSerializesJob.new("g", i + 1) }

    failing_insert_all(on_attempt: 2, error: ActiveRecord::Deadlocked) do
      assert_equal 3, SolidQueue::Job.enqueue_all(jobs)
    end

    assert_equal [ :collected, :created, :collected ], jobs.map(&:coalesce_status)
    created = SolidQueue::Job.where.not(id: open_job).sole
    assert_equal [ open_job, created.id, created.id ], jobs.map(&:provider_job_id)
    assert_equal jobs.second.job_id, created.active_job_id
    assert_nil created.arguments["provider_job_id"], "the failed attempt's job id can't leak into the new job"

    stored = SolidQueue::CoalescedCall.where(job_id: created.id).order(:id).map(&:arguments)
    assert_equal [ jobs.second.job_id, jobs.third.job_id ], stored.map { |payload| payload["job_id"] }
    assert_equal [ 1, 1 ], stored.map { |payload| payload["serialize_count"] }, "a retry reuses the payload it took the first time"
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "a failed admission rolls back the job, the key and the call" do
    SolidQueue::CoalescedCall.stubs(:insert_all).raises(ActiveRecord::StatementInvalid, "boom")

    assert_no_difference -> { SolidQueue::Job.count + SolidQueue::CoalesceKey.count } do
      assert_raises(SolidQueue::Job::EnqueueError) { TwoCallsJob.perform_later("g", 0) }
    end
  end

  test "a failed join leaves the open job's count and rows alone" do
    TwoCallsJob.perform_later("g", 0)

    failing_insert_all(on_attempt: 1, error: ActiveRecord::StatementInvalid) do
      assert_raises(SolidQueue::Job::EnqueueError) { TwoCallsJob.perform_later("g", 1) }
    end

    assert_equal 1, SolidQueue::CoalesceKey.sole.calls_count
    assert_equal 1, SolidQueue::CoalescedCall.count
  end

  test "a failed bulk enqueue keeps the jobs that committed, and only those report an outcome" do
    jobs = 5.times.map { |i| TwoCallsJob.new("g", i) }

    failing_insert_all(on_attempt: 2, error: ActiveRecord::StatementInvalid) do
      assert_raises(ActiveRecord::StatementInvalid) { SolidQueue::Job.enqueue_all(jobs) }
    end

    assert_equal [ :created, :collected, nil, nil, nil ], jobs.map(&:coalesce_status)
    assert_equal 1, SolidQueue::Job.count
    assert_equal [ true, true, false, false, false ], jobs.map { |active_job| active_job.successfully_enqueued? || false }
    assert_equal [ nil, nil, nil ], jobs.last(3).map(&:provider_job_id)
  end

  test "bulk enqueue in the app's transaction rolls back with it, every job included" do
    jobs = 5.times.map { |i| TwoCallsJob.new("g", i) }

    SolidQueue::Record.transaction do
      SolidQueue::Job.enqueue_all(jobs)
      raise ActiveRecord::Rollback
    end

    assert_equal 0, SolidQueue::Job.count + SolidQueue::CoalesceKey.count + SolidQueue::CoalescedCall.count
  end

  test "a deleted record's call is skipped with an event and retained with the others" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    skipped_id = SolidQueue::CoalescedCall.order(:id).first.id
    @a.destroy!

    events = SolidQueue.with(preserve_finished_jobs: true) do
      capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }
    end

    assert_equal [ [ [ @b ] ] ], JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert_equal 2, job.coalesced_calls.count
    assert SolidQueue::CoalescedCall.exists?(skipped_id)

    payload = events.sole.payload
    assert_equal skipped_id, payload[:call_id]
    assert_equal job.id, payload[:job_id]
    assert_equal job.active_job_id, payload[:active_job_id]
    assert_operator payload[:exception_class].constantize, :<=, ActiveJob::DeserializationError
    assert_equal %i[ active_job_id call_id class_name exception_class job_id ], payload.keys.sort
  end

  test "perform_calls and its callbacks are skipped when no calls remain, and the job finishes" do
    ExposingCallbacksJob.perform_later(@a)
    @a.destroy!

    perform_ready_jobs

    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
  end

  test "a job whose calls are all gone doesn't run a call on the re-run after a crash" do
    RecordsCollectedJob.perform_later(@a)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    # The run skips the call, and the worker dies before recording the job as finished
    claimed.send(:execute)
    claimed.release
    perform_ready_jobs

    assert_empty JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert job.reload.finished?
  end

  test "a missing record is skipped and reported again on the re-run after a crash" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    claimed.send(:execute)
    claimed.release
    events = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ [ [ @b ] ], [ [ @b ] ] ], JobBuffer.values
    assert_equal 1, events.size
    assert job.reload.finished?
  end

  test "a record restored after being skipped can run on the next attempt" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    rows = job.coalesced_calls.order(:id).pluck(:id, :arguments)
    @a.destroy!
    process = SolidQueue::Process.register(kind: "Worker", pid: 42, name: "crashing-worker")
    claimed = SolidQueue::ReadyExecution.claim("*", 10, process.id).sole

    claimed.send(:execute)
    assert_equal rows, job.coalesced_calls.order(:id).pluck(:id, :arguments)
    restored = JobResult.create!(id: @a.id)
    claimed.release
    events = capture_events("drop_coalesced_call.solid_queue") { perform_ready_jobs }

    assert_equal [ [ [ @b ] ], [ [ restored ], [ @b ] ] ], JobBuffer.values
    assert_empty events
    assert job.reload.finished?
  end

  test "a job with no calls from before it collected runs one call from its own arguments" do
    job = SolidQueue::Job.create!(
      queue_name: "default", class_name: "CollectedCallsJob", active_job_id: SecureRandom.uuid, scheduled_at: Time.current,
      arguments: CollectedCallsJob.new("g", 1).serialize
    )

    perform_ready_jobs

    assert_equal [ [ [ "g", 1 ] ] ], JobBuffer.values
    assert job.reload.finished?
  end

  test "a deserialization error that isn't a deleted record fails the run and keeps the calls" do
    RecordsCollectedJob.perform_later(@a)
    RecordsCollectedJob.perform_later(@b)
    job = SolidQueue::Job.sole
    @a.destroy!
    # Rails main looks records up with fetch instead of locate
    GlobalID::Locator.stubs(:locate).raises(ActiveRecord::ConnectionNotEstablished, "gone")
    GlobalID::Locator.stubs(:fetch).raises(ActiveRecord::ConnectionNotEstablished, "gone")

    events = capture_events("drop_coalesced_call.solid_queue") do
      assert_raises(ActiveJob::DeserializationError) { perform_ready_jobs }
    end
    GlobalID::Locator.unstub(:locate, :fetch)

    assert job.reload.failed?
    assert_equal 2, job.coalesced_calls.count
    assert_empty JobBuffer.values
    assert_empty events
  end

  test "a sealed job isn't broken by a deleted record in its own arguments, and neither is the next one" do
    TwoRecordsJob.perform_later(@a)
    TwoRecordsJob.perform_later(@a)
    TwoRecordsJob.perform_later(@b)
    TwoRecordsJob.perform_later(@a)
    jobs = SolidQueue::Job.order(:id).to_a
    rows = jobs.map { |job| job.coalesced_calls.order(:id).pluck(:id, :arguments) }
    @a.destroy!

    # The first job skips both calls. The second has @b's arguments, and skips @a's call
    SolidQueue.with(preserve_finished_jobs: true) { perform_ready_jobs }

    assert_equal [ [ [ @b ] ] ], JobBuffer.values
    assert_empty SolidQueue::FailedExecution.all
    assert_equal 0, SolidQueue::Job.where(finished_at: nil).count
    assert_equal rows, jobs.map { |job| job.coalesced_calls.order(:id).pluck(:id, :arguments) }
  end

  test "perform_now gets one call with no id and writes nothing" do
    assert_no_difference -> { SolidQueue::CoalescedCall.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      InlineCollectedJob.perform_now(@a, force: true)
    end

    call = JobBuffer.values.sole.sole
    assert_nil call.id
    assert_equal [ @a, { force: true } ], call.arguments
    assert Hash.ruby2_keywords_hash?(call.arguments.last)
    assert_equal InlineCollectedJob.name, call.job_data["job_class"]
    assert call.job_data.frozen?
    assert_same call.job_data, call.job_data
  end

  test "perform_now with arguments that can't be serialized still runs" do
    unserializable = Object.new

    InlineCollectedJob.perform_now(unserializable)
    CoalescedRefreshJob.perform_now(unserializable)

    call = JobBuffer.values.first.sole
    assert_same unserializable, call.arguments.first
    assert_raises(ActiveJob::SerializationError) { call.job_data }
    assert_same unserializable, JobBuffer.values.last
  end

  test "the inline adapter gets one call" do
    InlineCollectedJob.perform_later(@a)

    call = JobBuffer.values.sole.sole
    assert_nil call.id
    assert_equal [ @a ], call.arguments
    assert_equal 0, SolidQueue::CoalescedCall.count
  end

  test "discarding an open job deletes its waiting calls" do
    first = CollectedCallsJob.set(wait: 1.hour).perform_later("g", 0)
    CollectedCallsJob.set(wait: 1.hour).perform_later("g", 1)
    assert_equal 2, SolidQueue::CoalescedCall.waiting.count

    SolidQueue::Job.find(first.provider_job_id).discard

    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "discarding a sealed job deletes only its own calls, and leaves the newer open job alone" do
    sealed = TwoCallsJob.set(wait: 1.hour).perform_later("g", 0).provider_job_id
    TwoCallsJob.set(wait: 1.hour).perform_later("g", 1)
    open = TwoCallsJob.set(wait: 1.hour).perform_later("g", 2).provider_job_id

    SolidQueue::Job.find(sealed).discard

    assert_equal [ open ], SolidQueue::CoalescedCall.pluck(:job_id)
    assert_equal open, SolidQueue::CoalesceKey.sole.job_id
  end

  test "deferred calls are collected when the app's transaction commits, and none when it rolls back" do
    skip "Needs Rails 7.2" unless ActiveJob::Base.respond_to?(:enqueue_after_transaction_commit=)

    JobResult.transaction do
      DeferredCollectedJob.perform_later("g", 0)
      raise ActiveRecord::Rollback
    end
    assert_equal 0, SolidQueue::CoalescedCall.count

    calls = nil
    JobResult.transaction do
      calls = 3.times.map { |i| DeferredCollectedJob.perform_later("g", i) }
      assert_equal 0, SolidQueue::CoalescedCall.count
      assert_equal [ nil, nil, nil ], calls.map(&:coalesce_status)
      assert_equal [ nil, nil, nil ], calls.map(&:provider_job_id)
      assert calls.all?(&:successfully_enqueued?)
    end

    assert_equal [ :created, :collected, :created ], calls.map(&:coalesce_status)
    assert_equal 3, SolidQueue::CoalescedCall.count
  end

  test "bulk deferred calls are collected when the app's transaction commits" do
    skip "Needs a Rails that defers perform_all_later" unless defined?(ActiveJob::EnqueueAfterTransactionCommit::ActiveJobMethods)

    calls = 3.times.map { |i| DeferredCollectedJob.new("g", i) }
    JobResult.transaction do
      ActiveJob.perform_all_later(calls)
      assert_equal 0, SolidQueue::CoalescedCall.count
    end

    assert_equal [ :created, :collected, :created ], calls.map(&:coalesce_status)
    assert_equal 3, SolidQueue::CoalescedCall.count
  end

  test "bulk discarding open and sealed jobs deletes their calls and frees only their keys" do
    TwoCallsJob.set(wait: 1.hour).perform_later("g", 0)
    TwoCallsJob.set(wait: 1.hour).perform_later("g", 1)
    TwoCallsJob.set(wait: 1.hour).perform_later("g", 2)

    SolidQueue::ScheduledExecution.discard_all_in_batches

    assert_equal 0, SolidQueue::Job.count
    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal 0, SolidQueue::CoalesceKey.count
  end

  test "bulk discarding some jobs leaves another job's key alone" do
    discarded = TwoCallsJob.set(wait: 1.hour).perform_later("g", 0).provider_job_id
    kept = TwoCallsJob.set(wait: 1.hour).perform_later("h", 0).provider_job_id

    SolidQueue::ScheduledExecution.discard_all_from_jobs(SolidQueue::Job.where(id: discarded))

    assert_equal kept, SolidQueue::CoalesceKey.sole.job_id
    assert_equal [ kept ], SolidQueue::CoalescedCall.pluck(:job_id)
  end

  test "clearing a queue frees the keys of its jobs, before releasing their semaphores" do
    LimitedCallsJob.perform_later("g", 0)
    queries = []
    callback = ->(*, payload) { queries << payload[:sql] if payload[:sql].match?(/\A(DELETE FROM .solid_queue_coalesce_keys|UPDATE .solid_queue_semaphores)/) }

    ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { SolidQueue::Queue.find_by_name("default").clear }

    assert_equal 0, SolidQueue::CoalesceKey.count
    assert_equal 0, SolidQueue::CoalescedCall.count
    assert_equal [ "DELETE", "UPDATE" ], queries.map { |sql| sql.split.first }
  end

  [ true, false ].each do |preserve|
    test "skipped calls follow job retention with preserve_finished_jobs #{preserve}" do
      RecordsCollectedJob.perform_later(@a)
      RecordsCollectedJob.perform_later(@b)
      job = SolidQueue::Job.sole
      rows = job.coalesced_calls.order(:id).pluck(:id, :arguments)
      @a.destroy!

      SolidQueue.with(preserve_finished_jobs: preserve) { perform_ready_jobs }

      assert_equal [ [ [ @b ] ] ], JobBuffer.values
      if preserve
        assert job.reload.finished?
        assert_equal rows, job.coalesced_calls.order(:id).pluck(:id, :arguments)
        SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)
      end

      assert_not SolidQueue::Job.exists?(job.id)
      assert_equal 0, SolidQueue::CoalescedCall.count
    end
  end

  test "clearing a finished job deletes its calls" do
    SolidQueue.with(preserve_finished_jobs: true) do
      first = CollectedCallsJob.perform_later("g", 0)
      perform_ready_jobs

      job = SolidQueue::Job.find(first.provider_job_id)
      assert job.finished?
      assert_equal 1, job.coalesced_calls.count

      SolidQueue::Job.clear_finished_in_batches(finished_before: 1.minute.from_now)

      assert_equal 0, SolidQueue::CoalescedCall.count
    end
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

    # The rows go in before the error, so what's checked afterwards is the rollback.
    def failing_insert_all(on_attempt:, error:)
      insert_all = SolidQueue::CoalescedCall.method(:insert_all)
      attempts = 0

      SolidQueue::CoalescedCall.define_singleton_method(:insert_all) do |rows, **options|
        insert_all.call(rows, **options).tap { raise error, "boom" if (attempts += 1) == on_attempt }
      end

      yield
    ensure
      SolidQueue::CoalescedCall.singleton_class.remove_method(:insert_all)
    end
end

class SolidQueue::CoalesceTestAdapterTest < ActiveSupport::TestCase
  include ActiveJob::TestHelper

  test "the test adapter gets one call, sees the call's own arguments and writes nothing" do
    a, b = JobResult.create!, JobResult.create!

    assert_no_difference -> { SolidQueue::CoalescedCall.count + SolidQueue::CoalesceKey.count + SolidQueue::Job.count } do
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ a ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(a) }
      assert_enqueued_with(job: SolidQueue::CoalesceCollectTest::RecordsCollectedJob, args: [ b ]) { SolidQueue::CoalesceCollectTest::RecordsCollectedJob.perform_later(b) }
      perform_enqueued_jobs
    end

    assert_equal [ [ [ a ] ], [ [ b ] ] ], JobBuffer.values
  end

  private
    # The dummy app configures Solid Queue for tests, which Active Job respects otherwise
    def queue_adapter_for_test
      ActiveJob::QueueAdapters::TestAdapter.new
    end
end
