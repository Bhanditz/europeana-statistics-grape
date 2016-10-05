# frozen_string_literal: true
require_relative './app/base'
require 'rack/cors'

if ['development', 'test'].include?(ENV['RACK_ENV'])
  require 'dotenv'
  Dotenv.load
end

#	TO-DO for final production deploy
# For now, let's set origins to *.
# Later we will moidfy it allow only rumi.io

use Rack::Cors do
  allow do
    origins '*'
    resource '*', headers: :any, methods: :any
  end
end

Rack::Utils.key_space_limit = 262_144
run Base
