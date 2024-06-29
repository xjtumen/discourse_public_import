-- [params]
-- int :min_id = 0

SELECT
   u.id,
   u.username,
   u.name,
   u.admin,
   u.moderator,
   u.trust_level
FROM users u
WHERE u.active
  AND u.approved
  AND NOT u.staged
  AND u.id > :min_id
