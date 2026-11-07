class CollectedResultsJob < ApplicationJob
  coalesce key: ->(group, *) { group }, on_conflict: :collect, max_calls: 5

  def perform_calls(calls)
    JobResult.create!(status: "collected", value: calls.map { |call| call.arguments.last }.to_json)
  end
end
