# frozen_string_literal: true

module SolidQueue
  # A row means "this job is the open job for this key": it exists only while its
  # job exists and hasn't started. job_id is null only between the insert and the
  # end of the admission that inserted it, so it's never visible to others.
  # calls_count is zero for a discarding job, which is the only kind there is.
  class CoalesceKey < Record
    # Enqueuing a job that uses +coalesce+ before the coalescing migration has run.
    class PendingMigrations < StandardError
      def initialize(message = "The coalescing schema hasn't been installed yet. Run `bin/rails solid_queue:update` to copy the pending migrations to your application, and then `bin/rails db:migrate` to run them")
        super
      end
    end

    belongs_to :job, optional: true

    class << self
      # Until the app has run the migration everything else works as before.
      def migrated?
        @migrated ||= table_exists?
      end

      # Returns the row for this key, inserted if nobody holds it, and locked so
      # a job start can't delete it until this transaction commits. A row
      # with a null job_id is ours: the caller has to create the job for it.
      def admit(key)
        loop do
          insert_unless_held(key)

          admitted = lock(share_lock).find_by(key: key)
          return admitted if admitted
        end
      end

      # Locks through the key's index, like admission. Going through the job_id
      # index, even by deleting by it, takes InnoDB's locks on this row's indexes in
      # the opposite order to admission, and that deadlocks.
      def lock_for_release(job_id)
        return unless migrated?

        if key = where(job_id: job_id).pick(:key)
          lock.find_by(key: key, job_id: job_id)
        end
      end

      # For jobs deleted in bulk, which skips their destroy callback. In the order of
      # the keys, so that two of these can't deadlock with each other.
      def release_all(job_ids)
        return unless migrated?

        where(job_id: job_ids).order(:key).pluck(:key, :job_id).each do |key, job_id|
          lock.find_by(key: key, job_id: job_id)&.destroy!
        end
      end

      private
        # MariaDB and MySQL before 8 don't have FOR SHARE, and this means the same on MySQL 8
        def share_lock
          connection_pool.with_connection { |connection| connection.adapter_name == "PostgreSQL" } ? "FOR SHARE" : "LOCK IN SHARE MODE"
        end

        # Needs the unique index on key. Skips a conflict instead of raising, because
        # a duplicate would abort an enclosing transaction on PostgreSQL. On MySQL it
        # also avoids a deadlock: a plain INSERT that hits a duplicate takes a shared
        # lock, and two of those on a row being deleted can't both go on to insert.
        # created_at is set here and not by the database, because MySQL would write
        # it in the server's time zone, whatever the app's is.
        def insert_unless_held(key)
          if supports_insert_conflict_target?
            insert({ key: key, created_at: Time.current }, unique_by: :key)
          else
            insert({ key: key, created_at: Time.current })
          end
        end
    end
  end
end
