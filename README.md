#### Setting up your Environment

* Install GIT
* Install RVM (http://rvm.io/)
* Install Ruby 2.1.0
* Install PostGreSQL 9.3.3. Create user with username: developer and password: developer

#### Important URLs

Install Ruby: https://www.digitalocean.com/community/articles/how-to-install-ruby-on-rails-on-ubuntu-12-04-lts-precise-pangolin-with-rvm

Install PostGreSQL: https://help.ubuntu.com/community/PostgreSQL

Configure PostGreSQL: http://www.pixelite.co.nz/article/installing-and-configuring-postgresql-91-ubuntu-1204-local-drupal-development

#### Common Commands

$ rvm list

$ ruby -v

$ psql --version

#### Getting Started

Login to pgAdmin and manually create database with name: rumi_datasets

$ git clone git@github.com:pykih/api.rumi.io.git

$ cd api.rumi.io

$ rackup
