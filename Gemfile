source 'https://rubygems.org'

gem 'connection_pool'
gem 'grape'
gem 'json'
gem 'logging'
gem 'pg'
gem 'pg_search', git: "https://github.com/Casecommons/pg_search.git"
gem 'rack'
gem 'rack-cors'

group :production, :test do
  gem 'puma'
end

group :development, :test do
  gem 'dotenv'
  gem 'foreman'
end

group :development do
  gem 'rubocop', '0.39.0', require: false # only update when Hound does
end
