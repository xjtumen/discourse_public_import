-- [params]
-- int :min_id = 0

SELECT
    t.id,
    c.name,
    c.id,
    t.title,
    t.excerpt,
    t.created_at,
    t.last_posted_at,
    t.updated_at,
    t.views,
    t.posts_count,
    t.like_count,
    t.user_id,
    t.last_post_user_id,
    (SELECT STRING_AGG(tag.name, ', ') FROM topic_tags tt JOIN tags tag ON tag.id = tt.tag_id WHERE tt.topic_id = t.id) AS all_tags
FROM topics t
JOIN categories c ON c.id = t.category_id
WHERE NOT c.read_restricted
  AND t.deleted_at IS NULL
  AND t.archetype = 'regular'
AND t.id > :min_id
ORDER BY t.id ASC
