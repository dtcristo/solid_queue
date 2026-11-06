class CollectedCallsJob < ApplicationJob
  coalesce key: ->(group, *, **) { group }, on_conflict: :collect, max_calls: 3

  def perform_calls(calls)
    JobBuffer.add(calls.map(&:arguments))
  end
end
