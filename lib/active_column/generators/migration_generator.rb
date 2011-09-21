require 'rails/generators'
require 'rails/generators/named_base'

module ActiveColumn
  module Generators
    class MigrationGenerator < Rails::Generators::NamedBase

      source_root File.expand_path("../templates", __FILE__)

      class_option :migration_path, :type => :string, :default => "", 
        :desc => "migration path for generated migration"
      
      def self.banner
        "rails g active_column:migration NAME"
      end

      def self.desc(description = nil)
<<EOF
Description:
  Create an empty Cassandra migration file in 'ks/migrate' by default.  Very similar to Rails database migrations.
  use migration-path option to specify a different directory under 'ks/migrate'
Example:
  `rails g active_column:migration CreateFooColumnFamily`
  `rails g active_column:migration CreateFooColumnFamily --migration-path=production-oltp`
EOF
      end

      def create
        migration_path = options.migration_path
        timestamp = Time.now.utc.strftime("%Y%m%d%H%M%S")
        template 'migration.rb.erb', "ks/migrate/#{migration_path}/#{timestamp}_#{file_name.underscore}.rb"
      end

    end
  end
end
