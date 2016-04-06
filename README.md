#### Setting up your Environment

Install Ruby 2.2.2

#### Getting Started

First, set the database details in the environment variable `DATABASE_URL`, e.g.

```
DATABASE_URL="postgres://exampleuser:examplepass@babar.elephantsql.com:5432/exampledb"
```

Then, execute:
```
git clone git@github.com:europeana/europeana-statistics-grape.git
cd europeana-statistics-grape
bundle install
bundle exec puma -C config/puma.rb
```