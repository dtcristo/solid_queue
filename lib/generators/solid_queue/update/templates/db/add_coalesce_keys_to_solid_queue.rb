class AddCoalesceKeysToSolidQueue < ActiveRecord::Migration[7.1]
  def change
    # Fresh installs create these with the base schema, so skip any that
    # already exist
    create_table :solid_queue_coalesce_keys, if_not_exists: true do |t|
      t.string :key, null: false
      t.bigint :job_id
      t.integer :calls_count, default: 0, null: false
      t.datetime :created_at, null: false

      t.index :key, unique: true
      t.index :job_id, unique: true
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end

    create_table :solid_queue_coalesced_calls, if_not_exists: true do |t|
      t.bigint :job_id, null: false
      t.text :arguments, null: false
      t.datetime :created_at, null: false

      t.index :job_id
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end
  end
end
