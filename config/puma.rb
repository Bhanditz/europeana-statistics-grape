workers Integer(ENV['WEB_CONCURRENCY'] || 2)
threads_count = Integer(ENV['MAX_THREADS'] || 5)
threads threads_count, threads_count

app_dir = File.expand_path("../..", __FILE__)

preload_app!

rackup DefaultRackup

rails_env = ENV['RACK_ENV'] || 'development'

if rails_env == 'development'
  port ENV['PORT'] || 3000 if rails_env == 'development'
else
  bind "unix://#{app_dir}/puma.sock"
end

environment rails_env