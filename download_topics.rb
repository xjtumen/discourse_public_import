require "json"
require "net/http"
require "uri"
require "sqlite3"
require "mini_sql"
require "cgi"

begin
  config = JSON.parse(File.read("config.json"))
rescue StandardError
  puts "Please create a file called .creds with your API KEY and USERNAME"
end

# Replace these values with your Discourse instance details
DISCOURSE_DOMAIN = config["domain"]
API_KEY = config["api_key"]
API_USERNAME = config["api_username"]
TOPIC_QUERY_ID = config["topics_query_id"]
USER_QUERY_ID = config["users_query_id"]
POST_QUERY_ID = config["posts_query_id"]
LIKES_QUERY_ID = config["likes_query_id"]

d = DateTime.now
d = d.strftime("%Y%m%d_%H%M%S")
sqlite_conn = SQLite3::Database.new("xjtu-men-public-dump_#{d}.db")
conn = MiniSql::Connection.get(sqlite_conn)

def run_report(query_id:, min_id: 0, limit:)
  params = CGI.escape({ min_id: min_id.to_s }.to_json)

  uri =
    URI(
      "https://#{DISCOURSE_DOMAIN}/admin/plugins/explorer/queries/#{query_id}/run?limit=#{limit}&params=#{params}"
    )
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true

  request = Net::HTTP::Post.new(uri.request_uri)
  request["Content-Type"] = "application/json"
  request["Api-Key"] = API_KEY
  request["Api-Username"] = API_USERNAME

  response = http.request(request)
  if response.code != "200"
    puts "Error: #{response.code} #{response.message}"
    puts response.body
    exit 1
  end

  JSON.parse(response.body)
end

def create_schema(conn)
  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS topics (
      id INTEGER PRIMARY KEY,
      category_name TEXT,
      category_id INTEGER,
      title TEXT,
      excerpt TEXT,
      created_at TEXT,
      last_posted_at TEXT,
      updated_at TEXT,
      views INTEGER,
      posts_count INTEGER,
      like_count INTEGER,
      user_id INTEGER,
      last_post_user_id INTEGER,
      tags TEXT
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS users(
      id INTEGER PRIMARY KEY,
      username TEXT,
      name TEXT,
      admin INTEGER,
      moderator INTEGER,
      trust_level INTEGER
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS posts(
      id INTEGER PRIMARY KEY,
      raw TEXT,
      cooked TEXT,
      post_number INTEGER,
      topic_id INTEGER,
      user_id INTEGER,
      created_at TEXT,
      updated_at TEXT,
      reply_to_post_number INTEGER,
      reply_to_user_id INTEGER,
      reply_count INTEGER,
      like_count INTEGER,
      word_count INTEGER
    )
  SQL

  conn.exec <<~SQL
    CREATE TABLE IF NOT EXISTS likes(
      post_id INTEGER,
      user_id INTEGER,
      created_at TEXT
    )
  SQL

  conn.exec(
    "create unique index IF NOT EXISTS idxLikes on likes(post_id,user_id)"
  )

  conn.exec(
    "create index IF NOT EXISTS idxTopic on posts(topic_id,post_number)"
  )
end

def insert_users(conn, rows)
  highest_id = 0
  users_loaded = 0

  conn.exec "BEGIN TRANSACTION"

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO users (
      id,
      username,
      name,
      admin,
      moderator,
      trust_level
    )
    VALUES (?, ?, ?, ?, ?, ?)
  SQL
    users_loaded += 1
    highest_id = row[0] if row[0] > highest_id
  end

  conn.exec "COMMIT TRANSACTION"

  { highest_id: highest_id, users_loaded: users_loaded }
end


def insert_posts(conn, rows)
  highest_id = 0
  posts_loaded = 0

  conn.exec "BEGIN TRANSACTION"

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO posts (
      id,
      raw,
      cooked,
      post_number,
      topic_id,
      user_id,
      created_at,
      updated_at,
      reply_to_post_number,
      reply_to_user_id,
      reply_count,
      like_count,
      word_count
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  SQL
    posts_loaded += 1
    highest_id = row[0] if row[0] > highest_id
  end

  conn.exec "COMMIT TRANSACTION"

  { highest_id: highest_id, posts_loaded: posts_loaded }
end

def insert_topics(conn, rows)
  highest_id = 0
  topics_loaded = 0

  conn.exec "BEGIN TRANSACTION"

  rows.each do |row|
    conn.exec <<~SQL, *row
    INSERT OR IGNORE INTO topics (
      id,
      category_name,
      category_id,
      title,
      excerpt,
      created_at,
      last_posted_at,
      updated_at,
      views,
      posts_count,
      like_count,
      user_id,
      last_post_user_id,
      tags
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  SQL
    topics_loaded += 1
    highest_id = row[0] if row[0] > highest_id
  end

  conn.exec "COMMIT TRANSACTION"

  { highest_id: highest_id, topics_loaded: topics_loaded }
end


def insert_likes(conn, json)
  result = { highest_id: 0, likes_loaded: 0 }

  conn.exec "BEGIN TRANSACTION"

  json["rows"].each do |row|
    conn.exec <<~SQL, *row
      -- id: ?
      INSERT OR IGNORE INTO likes(post_id, user_id, created_at)
      VALUES (?, ?, ?)
    SQL
    result[:highest_id] = row[0] if row[0] > result[:highest_id]
    result[:likes_loaded] += 1
  end

  conn.exec "COMMIT TRANSACTION"

  result
end

def dl_users(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: USER_QUERY_ID, min_id: min_id, limit: 10_000)

    result = insert_users(conn, response_data["rows"])

    puts "Loaded #{result[:users_loaded]} users (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:users_loaded] == 0
  end
end

def dl_topics(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: TOPIC_QUERY_ID, min_id: min_id, limit: 10_000)

    result = insert_topics(conn, response_data["rows"])
    puts "Loaded #{result[:topics_loaded]} topics (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:topics_loaded] == 0
  end
end

def dl_posts(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: POST_QUERY_ID, min_id: min_id, limit: 10_000)

    result = insert_posts(conn, response_data["rows"])
    puts "Loaded #{result[:posts_loaded]} posts (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:posts_loaded] == 0
  end
end

def dl_likes(conn)
  min_id = 0
  while true
    response_data =
      run_report(query_id: LIKES_QUERY_ID, min_id: min_id, limit: 10_000)

    result = insert_likes(conn, response_data)

    puts "Loaded #{result[:likes_loaded]} likes (highest id is #{result[:highest_id]})"

    min_id = result[:highest_id]
    break if result[:likes_loaded] == 0
  end
end

create_schema(conn)
dl_users(conn)
dl_topics(conn)
dl_posts(conn)
dl_likes(conn)
