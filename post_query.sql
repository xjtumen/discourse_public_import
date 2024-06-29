-- [params]
-- int :min_id = 0

SELECT
   p.id,
   p.raw,
   p.cooked,
   p.post_number,
   p.topic_id,
   p.user_id,
   p.created_at,
   p.updated_at,
   p.reply_to_post_number,
   p.reply_to_user_id,
   p.reply_count,
   t.like_count,
   t.word_count
FROM topics t
JOIN posts p ON p.topic_id = t.id
JOIN categories c ON c.id = t.category_id
WHERE NOT c.read_restricted
  AND t.deleted_at IS NULL
  AND p.deleted_at IS NULL
  AND p.post_type = 1
  AND NOT p.hidden
AND p.id > :min_id
ORDER BY p.id ASC

