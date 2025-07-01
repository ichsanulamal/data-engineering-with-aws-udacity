-- 1. Top 10 Most Played Songs
SELECT 
    s.title,
    a.name as artist_name,
    COUNT(*) as play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY play_count DESC
LIMIT 10;

-- 2. User Activity by Hour of Day
SELECT 
    t.hour,
    COUNT(*) as plays,
    COUNT(DISTINCT sp.user_id) as unique_users
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY t.hour;

-- 3. Monthly Growth in Song Plays
SELECT 
    t.year,
    t.month,
    COUNT(*) as total_plays,
    COUNT(DISTINCT sp.user_id) as active_users,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY t.year, t.month) as growth
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- 4. Top Artists by Location
SELECT 
    a.location as artist_location,
    a.name as artist_name,
    COUNT(*) as play_count
FROM songplays sp
JOIN artists a ON sp.artist_id = a.artist_id
WHERE a.location IS NOT NULL
GROUP BY a.location, a.name
ORDER BY play_count DESC
LIMIT 15;

-- 5. User Engagement: Free vs Paid Users
SELECT 
    u.level,
    COUNT(*) as total_plays,
    COUNT(DISTINCT u.user_id) as unique_users,
    ROUND(COUNT(*)::float / COUNT(DISTINCT u.user_id), 2) as avg_plays_per_user
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
GROUP BY u.level;

-- 6. Weekend vs Weekday Listening Patterns
SELECT 
    CASE 
        WHEN t.weekday IN (5, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as total_plays,
    ROUND(AVG(COUNT(*)) OVER(), 2) as avg_plays
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY day_type;

-- 7. User Demographics Analysis
SELECT 
    u.gender,
    u.level,
    COUNT(DISTINCT u.user_id) as user_count,
    COUNT(*) as total_plays,
    ROUND(COUNT(*)::float / COUNT(DISTINCT u.user_id), 2) as avg_plays_per_user
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
GROUP BY u.gender, u.level
ORDER BY total_plays DESC;

-- 8. Geographic Distribution of Listening
SELECT 
    sp.location as user_location,
    COUNT(*) as play_count,
    COUNT(DISTINCT sp.user_id) as unique_users,
    COUNT(DISTINCT sp.artist_id) as unique_artists
FROM songplays sp
WHERE sp.location IS NOT NULL
GROUP BY sp.location
ORDER BY play_count DESC
LIMIT 20;

-- 9. Song Duration Preference Analysis
SELECT 
    CASE 
        WHEN s.duration < 180 THEN 'Short (< 3 min)'
        WHEN s.duration BETWEEN 180 AND 240 THEN 'Medium (3-4 min)'
        WHEN s.duration BETWEEN 240 AND 300 THEN 'Long (4-5 min)'
        ELSE 'Very Long (> 5 min)'
    END as duration_category,
    COUNT(*) as play_count,
    ROUND(AVG(s.duration), 2) as avg_duration
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
WHERE s.duration IS NOT NULL
GROUP BY duration_category
ORDER BY play_count DESC;

-- 10. Cohort Analysis: User Retention by Registration Month
WITH user_first_play AS (
    SELECT 
        sp.user_id,
        MIN(t.year * 100 + t.month) as first_play_month
    FROM songplays sp
    JOIN time t ON sp.start_time = t.start_time
    GROUP BY sp.user_id
),
monthly_activity AS (
    SELECT 
        ufp.first_play_month,
        t.year * 100 + t.month as activity_month,
        COUNT(DISTINCT sp.user_id) as active_users
    FROM songplays sp
    JOIN time t ON sp.start_time = t.start_time
    JOIN user_first_play ufp ON sp.user_id = ufp.user_id
    GROUP BY ufp.first_play_month, t.year * 100 + t.month
)
SELECT 
    first_play_month,
    activity_month,
    active_users,
    activity_month - first_play_month as months_since_first_play
FROM monthly_activity
WHERE months_since_first_play <= 12
ORDER BY first_play_month, activity_month;

-- 11. Peak Usage Times
SELECT 
    t.weekday,
    t.hour,
    COUNT(*) as play_count,
    RANK() OVER (PARTITION BY t.weekday ORDER BY COUNT(*) DESC) as hour_rank
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.weekday, t.hour
ORDER BY t.weekday, play_count DESC;

-- 12. User Session Analysis
SELECT 
    sp.session_id,
    sp.user_id,
    u.level,
    COUNT(*) as songs_in_session,
    MIN(sp.start_time) as session_start,
    MAX(sp.start_time) as session_end,
    EXTRACT(EPOCH FROM (MAX(sp.start_time) - MIN(sp.start_time)))/60 as session_duration_minutes
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
GROUP BY sp.session_id, sp.user_id, u.level
HAVING COUNT(*) > 1
ORDER BY songs_in_session DESC
LIMIT 20;