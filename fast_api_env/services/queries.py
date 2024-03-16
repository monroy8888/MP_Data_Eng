queries = {
    'prints_click': """SELECT *, 
        CASE 
            WHEN position_prints != 0 THEN TRUE 
            ELSE FALSE 
        END AS click 
        FROM prints;""",
    'prints_view_quantity': """SELECT 
        user_id,
        value_prop_prints,
        DATE_TRUNC('week', day_prints) AS three_weeks,
        COUNT(*) AS view_quantity
        FROM prints
        GROUP BY
            user_id,
            value_prop_prints,
            DATE_TRUNC('week', day_prints);""",
    'taps_clicks_quantity': """SELECT 
        user_id,
        value_prop_taps,
        DATE_TRUNC('week', day_taps) AS three_weeks,
        COUNT(*) AS clicks_quantity 
        FROM taps 
        GROUP BY
            user_id,
            value_prop_taps,
            DATE_TRUNC('week', day_taps);""",
    'pays_count': """SELECT 
        user_id,
        value_prop,
        DATE_TRUNC('week', pay_date) AS three_weeks,
        COUNT(*) AS pays_count
        FROM pays
        GROUP BY
            user_id,
            value_prop,
            DATE_TRUNC('week', pay_date);""",
    'pays_total': """SELECT 
        user_id,
        value_prop,
        DATE_TRUNC('week', pay_date) AS three_weeks,
        SUM(total) AS pays
        FROM pays
        GROUP BY
            user_id,
            value_prop,
            DATE_TRUNC('week', pay_date);
    """
}

