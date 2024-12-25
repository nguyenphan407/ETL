{% macro genarate_film_ratings() %}

WITH films_with_ratings AS (
    SELECT 
        film_id,
        title,
        release_date,
        price,
        rating,
        user_rating,
        CASE
            WHEN user_rating >= 4.5 THEN 'Excellent'
            WHEN user_rating >= 4.0 THEN 'Good'
            WHEN user_rating >= 3.0 THEN 'Average'
            ELSE 'Poor'
        END AS rating_category
    FROM {{ ref('films') }}
),

films_with_actors AS (
    SELECT
        f.film_id,
        f.title,
        STRING_AGG(a.actor_name, ',') AS actors
    FROM {{ ref('films') }} f
    LEFT JOIN {{ 'film_actors' }} fa ON fa.film_id = f.film_id
    LEFT JOIN {{ 'actors' }} a ON a.actor_id = fa.actor_id
    GROUP BY  f.film_id, f.title
)

SELECT
    fwf.*,
    fwa.actors
FROM films_with_ratings fwf
LEFT JOIN films_with_actors fwa ON fwf.film_id = fwa.film_id


{% endmacro %}