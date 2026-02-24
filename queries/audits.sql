CREATE OR REPLACE TEMP TABLE cqa_target_sends AS
WITH filtered_events AS (
    SELECT
        ae.event
        , ae.subscriber_id
        , ae.agent_id
        , ae.session_id
        , ae.send_id
        , ae.inbound_messages_bi_id
        , ae.chat_id
        , CONVERT_TIMEZONE('UTC', 'America/New_York', ae.created) AS created_time
        , s.company_id
        , s.user_id
        , a.agent_email
        , CONCAT(a.agent_first_name, ' ', a.agent_last_name) AS agent_name
    FROM dw_concierge.dim_agent_events ae
    LEFT JOIN dw_concierge.fact_concierge_sessions s ON s.session_id = ae.session_id
    LEFT JOIN dw_concierge.dim_agent a ON a.agent_id = ae.agent_id
    LEFT JOIN concierge.agent_outbound_messages o ON o.send_id = ae.send_id
    LEFT JOIN concierge.message_feedback f ON f.send_id = ae.send_id
    LEFT JOIN dw.dim_subscription sub ON sub.subscriber_id = s.subscriber_id
    LEFT JOIN dw_concierge.fact_concierge_chats fcs ON fcs.chat_id = ae.chat_id
    WHERE
        ae.event IN ('SEND', 'RECEIVE')
        AND s.participating_agent_list <> '62551'
        AND s.participating_agent_list <> '38773'
        AND s.participating_agent_list <> ''
        AND o.autosend_type IS NULL
        AND f.id IS NULL
        AND a.agent_end_date IS NULL
        AND a.platform <> 'Flex'
        AND NOT sub.is_subscription_opted_out
        AND fcs.started_reason = 'AUTO_ASSIGNMENT'
        AND sub.subscription_channel = 'TEXT'
        AND created_time >= DATEADD(week, -1, DATE_TRUNC('WEEK', CURRENT_DATE))
        AND created_time <  DATE_TRUNC('WEEK', CURRENT_DATE)
),

events AS (
    SELECT
        CONCAT('https://ui.attentivemobile.com/concierge/chat/', BASE64_ENCODE(CONCAT('04:User', BASE64_ENCODE(CONCAT(fe.company_id,':',fe.user_id))))) as conversation_url
        , fe.subscriber_id
        , fe.company_id
        , fe.event
        , fe.agent_email
        , fe.agent_name
        , fe.created_time AS event_time
        , fe.send_id
        , fe.session_id
        , fe.inbound_messages_bi_id
        , fe.chat_id
        , LAG(fe.event, 1) OVER (PARTITION BY fe.session_id ORDER BY fe.created_time) AS previous_event
        , LAG(fe.inbound_messages_bi_id, 1) OVER (PARTITION BY fe.session_id ORDER BY fe.created_time) AS previous_receive_id
    FROM filtered_events fe
),

final_events AS (
    SELECT DISTINCT
        e.event
        , e.subscriber_id
        , e.company_id
        , e.conversation_url
        , e.agent_email
        , e.agent_name
        , e.event_time
        , e.send_id
        , e.session_id
        , e.inbound_messages_bi_id
        , e.chat_id
        , e.previous_event
        , e.previous_receive_id
    FROM events e
    JOIN dw_concierge.fact_concierge_chats ch
        ON ch.chat_id = e.chat_id
    WHERE
        ch.event_list NOT ILIKE '%OPT_OUT%'
        ch.event_list NOT ILIKE 'ADD_TO_BLOCKLIST'
),

prices AS (
    SELECT
        fe.event
        , fe.send_id
        , fe.previous_receive_id 
        , ROW_NUMBER() OVER(PARTITION BY fe.send_id ORDER BY fe.event_time) AS rn
    FROM final_events fe
    WHERE
        fe.event = 'SEND'
        AND previous_event = 'RECEIVE'
),

message_body AS (
    SELECT DISTINCT
        e.conversation_url
        , e.subscriber_id
        , e.company_id
        , e.event_time
        , e.send_id
        , e.agent_email
        , e.agent_name
        , inb.message_body AS receive_text
        , aom.body AS send_text
        , ROW_NUMBER() OVER (PARTITION BY e.company_id ORDER BY RANDOM()) AS company_id_num
    FROM final_events e
    LEFT JOIN concierge.inbound_messages inb
        ON inb.bi_id = e.previous_receive_id
    LEFT JOIN concierge.agent_outbound_messages aom
        ON aom.send_id = e.send_id
    LEFT JOIN prices p
        ON p.send_id = e.send_id
    WHERE
        e.event = 'SEND'
        
        -- 1. Exclude assistant-like replies
        AND NOT (
            aom.body ILIKE '%may I help%'
            OR aom.body ILIKE '%can you clarify%'
            OR aom.body ILIKE '%may I assist%'
            OR aom.body ILIKE '%can I do for you%'
            OR aom.body ILIKE '%can I help%'
            OR aom.body ILIKE '%can we help%'
            OR aom.body ILIKE '%can I assist%'
            OR aom.body ILIKE '%meant for me%'
            OR aom.body ILIKE '%intended for me%'
            OR aom.body ILIKE '%system provides you with personalized exclusive content%'
            OR aom.body ILIKE '%be sure to send you personalized exclusive content%'
            OR aom.body ILIKE '%limit the number of emails we send out%'
            OR aom.body ILIKE '%limit the number of texts we send out because we do%'
            OR aom.body ILIKE 'You''re welcome. I''ll be in touch.'
            OR aom.body ILIKE 'You''re welcome. I''ll be in touch!'
            OR aom.body ILIKE 'My pleasure!'
            OR aom.body ILIKE 'You''re welcome!'
        )
        -- 2. Exclude image/media responses ONLY IF inbound message is NULL
        AND NOT (
            inb.message_body IS NULL
            AND (
                aom.body ILIKE '%can''t open it on my end%'
                OR aom.body ILIKE '%try to send a photo%'
                OR aom.body ILIKE '%access the image%'
                OR aom.body ILIKE '%try to send an image%'
                OR aom.body ILIKE '%sending it over again%'
                OR aom.body ILIKE '%can see you sent a photo%'
                OR aom.body ILIKE '%can see you sent an image%'
            )
        )
        -- 3. Exclude generic sign-offs unless exception applies
        AND NOT (
            aom.body ILIKE '%feel free to reach out%'
            OR aom.body ILIKE '%have a great day%'
            OR aom.body ILIKE '%have a great evening%'
            OR aom.body ILIKE '%have a great weekend%'
            OR aom.body ILIKE 'No worries! Is there anything else I can help you with today?'
            OR aom.body ILIKE 'Sure! What questions do you have?'
            OR aom.body ILIKE '%Thanks for your patience%'
        )
        OR (
            aom.body ILIKE '%team%'
            OR aom.body ILIKE '%http%'
            OR inb.message_body ILIKE '%?%'
        )
        -- 4. Exclude price-related first messages
        AND NOT (
            p.rn = 1
            AND LOWER(RTRIM(inb.message_body, '?')) IN (
                'price',
                'how much is it',
                'how much',
                'how much longer',
                'how much are they',
                'how much does it cost',
                'the price',
                'what''s the price',
                'prices'
            )
            AND aom.body NOT ILIKE '%http%'
        )
        -- 5. Exclude identity-checking questions
        AND NOT (
            inb.message_body IS NOT NULL
            AND LOWER(RTRIM(inb.message_body, '?')) IN (
                'what is your name',
                'what''s your name',
                'are you a bot',
                'are you a robot',
                'are you an ai'
            )
        )
        -- 6. Exclude replies to support/help only
        AND NOT (
            LOWER(TRIM(inb.message_body)) IN ('support', 'help')
        )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.session_id
        ORDER BY e.event_time DESC, e.send_id DESC
    ) = 1
),

individual_sends AS (
    SELECT
        a.agent_email
        , COUNT(CASE WHEN ae.send_id IS NOT NULL THEN 1 ELSE NULL END) AS send_count
    FROM filtered_events ae
    LEFT JOIN dw_concierge.dim_agent a
        ON a.agent_id = ae.agent_id
    WHERE
        ae.agent_id NOT IN (62551, 38773)
    GROUP BY
        a.agent_email
),

agent_sends AS (
    SELECT
        agent_email
        , SUM(send_count) AS agent_send_count
    FROM individual_sends
    GROUP BY
        agent_email
),

total_sends AS (
    SELECT
        SUM(send_count) AS total_sends
        , 750 AS capacity
    FROM individual_sends
    GROUP BY
        capacity
),

audit_count AS (
    SELECT
        ags.*
        , ts.*
        , CASE WHEN ags.agent_email = 'sbertrand@concierge.attentivemobile.com' THEN 6
            WHEN ROUND(ags.agent_send_count/ts.total_sends*ts.capacity, 0) > 20 THEN 20
            WHEN ROUND(ags.agent_send_count/ts.total_sends*ts.capacity, 0) < 6 THEN 6
            ELSE ROUND(ags.agent_send_count/ts.total_sends*ts.capacity, 0)
        END AS audits_needed
    FROM agent_sends ags
    JOIN total_sends ts
    GROUP BY
        ags.agent_email
        , ags.agent_send_count
        , ts.total_sends
        , ts.capacity
),

filtered_sends AS (
    SELECT
        *
    FROM message_body
    WHERE
        company_id_num <= 300
),

one_send_id AS (
    SELECT
        fs.*
        , ROW_NUMBER() OVER(PARTITION BY fs.send_id ORDER BY fs.send_id) AS row_num
    FROM filtered_sends fs
),

deduped_sends AS (
    SELECT
        fs.*
        , ROW_NUMBER() OVER(PARTITION BY fs.agent_email ORDER BY HASH(fs.send_id)) AS rn
    FROM one_send_id fs
    WHERE
        fs.row_num = 1
),

final_results AS (
    SELECT
        ds.conversation_url
        , REGEXP_REPLACE(ds.send_text, '^[^:]+:\s*', '') AS send_text
        , ds.send_id
        , INITCAP(TO_CHAR(ds.event_time, 'MON')) || ' ' ||
            TO_NUMBER(TO_CHAR(ds.event_time, 'DD')) || ', ' ||
            TO_CHAR(ds.event_time, 'YYYY') || ', ' ||
            LTRIM(TO_CHAR(ds.event_time, 'HH12:MI'), '0') || ' ' ||
            LOWER(TO_CHAR(ds.event_time, 'AM')) AS send_time
        , ds.agent_name
        , ds.event_time
        , ds.company_id
        , ds.subscriber_id
    FROM deduped_sends ds
    INNER JOIN audit_count ac
        ON ac.agent_email = ds.agent_email
    WHERE
        ds.rn <= ac.audits_needed
    ORDER BY
        ds.agent_email
        , ds.rn
)

SELECT
    send_id
FROM final_results f
ORDER BY send_id;

/*==============================================================
1) CONTEXT TABLES (UTC predicates, EST for display)
==============================================================*/
CREATE OR REPLACE TEMP TABLE cqa_send_bounds AS
WITH target_send_events AS (
    SELECT
        dae.session_id,
        dae.send_id,
        dae.created AS send_created_utc,
        CONVERT_TIMEZONE('UTC','America/New_York', dae.created) AS send_created_est,
        dae.subscriber_id,
        dae.company_id,
        dae.user_id
    FROM dw_concierge.dim_agent_events dae
    JOIN cqa_target_sends ts
        ON ts.send_id = dae.send_id
    WHERE dae.event = 'SEND'
),

base_events AS (
    SELECT
        tse.session_id,
        tse.send_id,
        tse.send_created_utc AS event_time_utc,
        tse.send_created_est AS event_time_est,
        tse.subscriber_id,
        tse.company_id,
        tse.user_id,
        'SEND' AS event,
        NULL::STRING AS escalation_note,
        NULL::STRING AS template_title,
        NULL::TIMESTAMP_NTZ AS template_last_updated,
        2 AS sort_key
    FROM target_send_events tse

    UNION ALL

    SELECT
        tse.session_id,
        tse.send_id,
        e.created AS event_time_utc,
        CONVERT_TIMEZONE('UTC','America/New_York', e.created) AS event_time_est,
        tse.subscriber_id,
        tse.company_id,
        tse.user_id,
        'ESCALATED' AS event,
        e.note AS escalation_note,
        NULL::STRING AS template_title,
        NULL::TIMESTAMP_NTZ AS template_last_updated,
        0 AS sort_key
    FROM target_send_events tse
    JOIN concierge.escalations e
        ON e.user_id = tse.user_id
    WHERE e.created BETWEEN DATEADD(day, -30, tse.send_created_utc) AND tse.send_created_utc

    UNION ALL

    SELECT
        tse.session_id,
        tse.send_id,
        tu.created AS event_time_utc,
        CONVERT_TIMEZONE('UTC','America/New_York', tu.created) AS event_time_est,
        tse.subscriber_id,
        tse.company_id,
        tse.user_id,
        'TEMPLATE_USED' AS event,
        NULL::STRING AS escalation_note,
        temp.title AS template_title,
        tl.last_updated AS template_last_updated,
        1 AS sort_key
    FROM target_send_events tse
    JOIN concierge.agent_template_usage tu
        ON tu.send_id = tse.send_id
    LEFT JOIN concierge.agent_templates temp
        ON temp.id = tu.template_id
    LEFT JOIN (
        SELECT template_id, MAX(created) AS last_updated
        FROM concierge.agent_template_events
        GROUP BY 1
    ) tl
        ON tl.template_id = tu.template_id
    WHERE tu.created <= tse.send_created_utc
),

send_anchor AS (
    SELECT
        send_id,
        MAX(IFF(event = 'SEND', event_time_utc, NULL)) AS send_time_utc,
        MAX(IFF(event = 'SEND', event_time_est, NULL)) AS send_time_est
    FROM base_events
    GROUP BY 1
)

SELECT
    be.session_id,
    be.send_id,
    sa.send_time_utc AS send_time_utc,
    DATEADD(day, -30, sa.send_time_utc) AS window_start_utc,
    sa.send_time_utc AS window_end_utc,
    sa.send_time_est AS send_time_est,
    be.subscriber_id,
    be.user_id,
    dc.company_name,
    be.company_id,
    CONCAT('https://', dc.company_domain) AS company_website,
    bvs.message_tone,
    bvs.agent_name AS persona,
    bvs.escalation_topics,
    bvs.blocklisted_words,
    cn.note AS company_notes,

    /* optional rollups you can use later */
    ARRAY_COMPACT(
        ARRAY_AGG(
            IFF(be.event = 'ESCALATED',
                OBJECT_CONSTRUCT(
                    'note', be.escalation_note,
                    'created_at_utc', be.event_time_utc
                ),
                NULL
            )
        ) WITHIN GROUP (ORDER BY be.event_time_utc ASC)
    ) AS escalation_notes,
    ARRAY_COMPACT(
      ARRAY_AGG(
        IFF(be.event='TEMPLATE_USED' AND be.template_title IS NOT NULL,
          OBJECT_CONSTRUCT(
            'template_title', be.template_title,
            'last_updated', be.template_last_updated,
            'used_at_utc', be.event_time_utc
      ),
      NULL
    )
  )
) AS template_used

FROM base_events be
JOIN send_anchor sa
    ON sa.send_id = be.send_id
LEFT JOIN dw.dim_company dc
    ON dc.company_id = be.company_id
LEFT JOIN concierge.brand_voice_settings bvs
    ON bvs.company_id = be.company_id
LEFT JOIN concierge.company_notes cn
    ON cn.company_id = be.company_id
GROUP BY
    be.session_id,
    be.send_id,
    sa.send_time_utc,
    sa.send_time_est,
    be.subscriber_id,
    be.user_id,
    dc.company_name,
    be.company_id,
    dc.company_domain,
    bvs.message_tone,
    bvs.agent_name,
    bvs.escalation_topics,
    bvs.blocklisted_words,
    cn.note
QUALIFY ROW_NUMBER() OVER (PARTITION BY be.send_id ORDER BY sa.send_time_utc DESC) = 1;

CREATE OR REPLACE TEMP TABLE subscribers_scoped AS
SELECT
send_id, session_id, subscriber_id, user_id,
send_time_utc, window_start_utc, window_end_utc, send_time_est
FROM cqa_send_bounds;

CREATE OR REPLACE TEMP TABLE cqa_global_window AS
SELECT
MIN(window_start_utc) AS min_start_utc,
MAX(window_end_utc) AS max_end_utc
FROM subscribers_scoped;

CREATE OR REPLACE TEMP TABLE cqa_subscriber_windows AS
SELECT subscriber_id, MIN(window_start_utc) AS min_start_utc, MAX(window_end_utc) AS max_end_utc
FROM subscribers_scoped
GROUP BY 1;

CREATE OR REPLACE TEMP TABLE cqa_session_windows AS
SELECT session_id, MIN(window_start_utc) AS min_start_utc, MAX(window_end_utc) AS max_end_utc
FROM subscribers_scoped
GROUP BY 1;

/*==============================================================
1.5) PRUNE HEAVY EVENT TABLES (biggest speedup)
==============================================================*/
CREATE OR REPLACE TEMP TABLE subs_in_scope AS
SELECT subscriber_id, user_id FROM subscribers_scoped;

/* Product views pruned to subs across all history (max 5 per user) */
CREATE OR REPLACE TEMP TABLE epv_pruned AS
SELECT
    s.user_id,
    epv.product_product_id,
    epv.product_name,
    epv.request_url,
    epv.event_datetime
FROM subs_in_scope s
INNER JOIN events.events_product_view epv
    ON epv.user_identity_matched_users_primary_user_match_user_id = s.user_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.user_id, epv.product_product_id
    ORDER BY epv.event_datetime DESC
) <= 5;
    
/* Purchases/orders pruned to subs across all history */
CREATE OR REPLACE TEMP TABLE ep_pruned AS
WITH products_agg AS (
    SELECT 
        p.user_identity_matched_users_primary_user_match_user_id AS user_id,
        p.cart_order_id AS order_id,
        p.cart_currency AS currency,
        p.cart_total AS total,
        p.cart_coupon AS coupon,
        p.cart_discount AS discount,
        p.event_datetime,
        ARRAY_AGG(DISTINCT
            OBJECT_CONSTRUCT(
                'product_currency', product_currency,
                'product_id', product_product_id,
                'product_link', pc.link,
                'product_name', product_name,
                'product_price', product_price
            )
        ) AS products,
        ROW_NUMBER() OVER (
            PARTITION BY p.user_identity_matched_users_primary_user_match_user_id
            ORDER BY p.event_datetime DESC
        ) AS rn
    FROM attentive.events.events_purchase p
    LEFT JOIN data_lake_prod.product_catalog.products pc
        ON pc.origin_id = p.product_product_id
    JOIN subs_in_scope s
        ON s.user_id = p.user_identity_matched_users_primary_user_match_user_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
order_links AS (
    SELECT     
        o.user_identity_matched_users_primary_user_match_user_id AS user_id,
        o.event_order_id AS order_id,
        o.event_order_number AS order_number,
        o.event_order_status_url AS order_status_url,
        o.event_tracking_url AS order_tracking_url,
        o.event_datetime,
        ROW_NUMBER() OVER (PARTITION BY o.user_identity_matched_users_primary_user_match_user_id, o.event_order_id ORDER BY o.event_datetime DESC) AS rn
    FROM attentive.events.events_order o
    JOIN subs_in_scope s
        ON s.user_id = o.user_identity_matched_users_primary_user_match_user_id
)
SELECT 
    p.user_id,
    p.order_id,
    p.event_datetime,
    ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'currency', p.currency,
            'order_id', p.order_id::VARCHAR,
            'order_number', COALESCE(ol.order_number, p.order_id::VARCHAR),
            'order_status_link', ol.order_status_url,
            'order_tracking_link', ol.order_tracking_url,
            'products', p.products,
            'coupon', p.coupon,
            'discount_amount', p.discount,
            'total', p.total
        )
    ) AS orders
FROM products_agg p
LEFT JOIN order_links ol
    ON p.user_id = ol.user_id
    AND p.order_id = ol.order_id
    AND ol.rn = 1
GROUP BY p.order_id, p.user_id, p.event_datetime;

/* MAIN QUERY */
WITH gw AS (
    SELECT
        min_start_utc,
        max_end_utc
    FROM cqa_global_window
),

/* ---- System (non-concierge) messages in subscriber windows ---- */
system_messages_pruned AS (
    SELECT
        'SYSTEM' AS event,
        CONVERT_TIMEZONE('UTC','America/New_York', emr.event_datetime) AS time_est,
        emr.subscriber_id,
        emr.event_message_text AS text
    FROM events.events_message_receipt emr
    WHERE
        emr.message_type <> 'CONCIERGE'
        AND emr.message_subtype <> 'CONCIERGE'
        AND emr.event_datetime BETWEEN (SELECT min_start_utc FROM gw) AND (SELECT max_end_utc FROM gw)
        AND EXISTS (
            SELECT 1
            FROM cqa_subscriber_windows sw
            WHERE
                sw.subscriber_id = emr.subscriber_id
                AND emr.event_datetime BETWEEN sw.min_start_utc AND sw.max_end_utc
        )
),

/* ---- Product views (<= send_time only), include view_date yyyy-mm-dd ---- */
epv_src AS (
    SELECT
        s.send_id,
        p.user_id,
        p.product_name,
        p.request_url,
        TO_CHAR(p.event_datetime::DATE, 'YYYY-MM-DD') AS view_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.send_id, p.user_id
            ORDER BY p.event_datetime DESC
        ) AS rn
    FROM subscribers_scoped s
    JOIN epv_pruned p
        ON p.user_id = s.user_id
        AND p.event_datetime < s.send_time_utc
),
last_5_products AS (
    SELECT
        send_id,
        user_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'product_name', product_name,
                'product_link', request_url,
                'view_date', view_date
            )
        ) WITHIN GROUP (ORDER BY rn) AS last_5_products
    FROM epv_src
    WHERE rn <= 5
    GROUP BY 1, 2
),

/* ---- Unified Orders (use ep_pruned as-built: flatten ep_pruned.orders; rank last 5 per send) ---- */
orders_src AS (
    SELECT
        s.send_id,
        s.user_id,
        ep.event_datetime AS order_time_utc,
        f.value::VARIANT AS order_obj
    FROM subscribers_scoped s
    JOIN ep_pruned ep
        ON ep.user_id = s.user_id
        AND ep.event_datetime < s.send_time_utc
    , LATERAL FLATTEN(input => ep.orders) f
),
orders_ranked AS (
    SELECT
        send_id,
        user_id,
        order_time_utc,
        OBJECT_INSERT(
            OBJECT_INSERT(
                order_obj,
                'order_date',
                TO_CHAR(order_time_utc::DATE, 'YYYY-MM-DD'),
                TRUE
            ),
            'date_time',
            order_time_utc,
            TRUE
        ) AS order_obj_with_date,
        ROW_NUMBER() OVER (
            PARTITION BY send_id, user_id
            ORDER BY order_time_utc DESC
        ) AS rn
    FROM orders_src
),
unified_orders AS (
    SELECT
        send_id,
        user_id,
        ARRAY_AGG(order_obj_with_date) WITHIN GROUP (ORDER BY rn) AS orders
    FROM orders_ranked
    WHERE rn <= 5
    GROUP BY 1, 2
),

/* ---- Coupon assignments (as JSON array; multiple rows per subscriber_id) ---- */
coupon_data AS (
    SELECT
        s.send_id,
        ARRAY_AGG(
            DISTINCT OBJECT_CONSTRUCT(
                'coupon', ca.coupon,
                'redeemed', ca.redeemed,
                'status', cs.status,
                'description', cs.description,
                'value', cs.value
            )
        ) AS coupons
    FROM subscribers_scoped s
    LEFT JOIN attentive.incentives.coupon_assignments ca
        ON ca.subscriber_id = s.subscriber_id
    LEFT JOIN attentive.incentives.coupon_sets cs
        ON cs.id = ca.coupon_set_id
    WHERE
        ca.coupon IS NOT NULL
        OR ca.redeemed IS NOT NULL
        OR cs.status IS NOT NULL
        OR cs.description IS NOT NULL
        OR cs.value IS NOT NULL
    GROUP BY 1
),

/* ---- Receive events (for images) pruned to global window ---- */
receive_events_pruned AS (
    SELECT
        re.subscriber_id,
        re.created,
        re.text,
        re.media
    FROM dw_events.receive_events re
    WHERE re.created BETWEEN (SELECT min_start_utc FROM gw) AND (SELECT max_end_utc FROM gw)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY re.subscriber_id, re.created, re.text, re.media
        ORDER BY re.created DESC
    ) = 1
),

/* ---- Images per inbound message (join on subscriber_id + message text) ---- */
inbound_message_images AS (
    SELECT
        s.send_id,
        s.session_id,
        s.subscriber_id,
        inb.bi_id AS inbound_messages_bi_id,
        ARRAY_AGG(re.media) AS images
    FROM subscribers_scoped s
    JOIN concierge.inbound_messages inb
        ON inb.subscriber_id = s.subscriber_id
    LEFT JOIN receive_events_pruned re
        ON re.subscriber_id = inb.subscriber_id
        AND re.text = inb.message_body
    WHERE
        re.media IS NOT NULL
        AND re.media <> '[]'
    GROUP BY 1,2,3,4
),

/* ---- Concierge events/messages ---- */
dae_pruned AS (
    SELECT
        d.session_id,
        d.event,
        d.created AS created_utc,
        d.inbound_messages_bi_id,
        d.send_id,
        d.agent_id,
        d.user_id
    FROM dw_concierge.dim_agent_events d
    JOIN cqa_session_windows sw
        ON sw.session_id = d.session_id
        AND d.created BETWEEN sw.min_start_utc AND sw.max_end_utc
    WHERE d.event IN ('SEND','RECEIVE')  -- templates/escalations now come from cqa_send_bounds rollups
),

event_msgs_receive AS (
    SELECT
        d.session_id,
        s.subscriber_id,
        'subscriber' AS role,
        'RECEIVE' AS event_type,
        CONVERT_TIMEZONE('UTC','America/New_York', d.created_utc) AS message_time,
        inb.message_body AS text,
        s.send_id AS send_id,
        NULL::STRING AS agent_send_id,
        imi.images AS images,
        NULL::NUMBER AS agent_id
    FROM dae_pruned d
    JOIN subscribers_scoped s
        ON s.session_id = d.session_id
        AND d.created_utc BETWEEN s.window_start_utc AND s.window_end_utc
    JOIN concierge.inbound_messages inb
        ON inb.bi_id = d.inbound_messages_bi_id
    LEFT JOIN inbound_message_images imi
        ON imi.send_id = s.send_id
        AND imi.session_id = d.session_id
        AND imi.subscriber_id = s.subscriber_id
        AND imi.inbound_messages_bi_id = inb.bi_id
    WHERE d.event = 'RECEIVE'
),

event_msgs_send AS (
    SELECT
        d.session_id,
        s.subscriber_id,
        'agent' AS role,
        'SEND' AS event_type,
        CONVERT_TIMEZONE('UTC','America/New_York', d.created_utc) AS message_time,
        aom.body AS text,
        s.send_id AS send_id,
        d.send_id AS agent_send_id,
        NULL::VARIANT AS images,
        d.agent_id
    FROM dae_pruned d
    JOIN subscribers_scoped s
        ON s.session_id = d.session_id
        AND d.created_utc <= DATEADD(second, 1, s.window_end_utc)
    JOIN concierge.agent_outbound_messages aom
        ON aom.send_id = d.send_id
    WHERE d.event = 'SEND'
),

/* ---- TEMPLATE_USED events (from cqa_send_bounds.template_used rollup) ---- */
template_events AS (
    SELECT
        sb.session_id,
        sb.subscriber_id,
        'template' AS role,
        'TEMPLATE_USED' AS event_type,
        CONVERT_TIMEZONE('UTC','America/New_York', tu.value:"used_at_utc"::TIMESTAMP_NTZ) AS message_time,
        CONCAT(
            'Template used: "',
            tu.value:"template_title"::STRING,
            '"; last updated: ',
            tu.value:"last_updated"::STRING
        ) AS text,
        sb.send_id AS send_id,
        NULL::STRING AS agent_send_id,
        NULL::VARIANT AS images,
        NULL::NUMBER AS agent_id
    FROM cqa_send_bounds sb,
         LATERAL FLATTEN(input => sb.template_used) tu
),

/* ---- ESCALATED events (from cqa_send_bounds.escalation_notes rollup) ---- */
escalation_events AS (
    SELECT
        sb.session_id,
        sb.subscriber_id,
        'escalation' AS role,
        'ESCALATED' AS event_type,
        CONVERT_TIMEZONE('UTC','America/New_York', en.value:"created_at_utc"::TIMESTAMP_NTZ) AS message_time,
        en.value:"note"::STRING AS text,
        sb.send_id AS send_id,
        NULL::STRING AS agent_send_id,
        NULL::VARIANT AS images,
        NULL::NUMBER AS agent_id
    FROM cqa_send_bounds sb,
         LATERAL FLATTEN(input => sb.escalation_notes) en
    WHERE
        en.value:"note" IS NOT NULL
),

system_msgs_windowed AS (
    SELECT
        s.session_id,
        s.subscriber_id,
        'system' AS role,
        sm.event AS event_type,
        sm.time_est AS message_time,
        sm.text AS text,
        s.send_id AS send_id,
        NULL::STRING AS agent_send_id,
        NULL::VARIANT AS images,
        NULL::NUMBER AS agent_id
    FROM system_messages_pruned sm
    JOIN subscribers_scoped s
        ON s.subscriber_id = sm.subscriber_id
        AND sm.time_est BETWEEN CONVERT_TIMEZONE('UTC','America/New_York', s.window_start_utc)
                          AND CONVERT_TIMEZONE('UTC','America/New_York', s.window_end_utc)
),

combined_msgs AS (
    SELECT * FROM event_msgs_receive
    UNION ALL
    SELECT * FROM event_msgs_send
    UNION ALL
    SELECT * FROM template_events
    UNION ALL
    SELECT * FROM escalation_events
    UNION ALL
    SELECT * FROM system_msgs_windowed
),

conversation_json AS (
    SELECT
        cm.send_id,
        cm.session_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'date_time', cm.message_time,
                'message_type',
                    CASE
                        WHEN cm.event_type = 'TEMPLATE_USED' THEN 'template'
                        WHEN cm.event_type = 'ESCALATED' THEN 'escalation'
                        WHEN cm.role = 'subscriber' THEN 'customer'
                        WHEN cm.role = 'agent' THEN 'agent'
                        ELSE 'system'
                    END,
                'message_text', cm.text,
                'message_media', COALESCE(cm.images, ARRAY_CONSTRUCT()),
                'message_id', COALESCE(cm.agent_send_id, NULL),
                'agent', COALESCE(cm.agent_id, NULL)
            )
        ) WITHIN GROUP (
            ORDER BY
                cm.message_time,
                CASE
                    WHEN cm.role = 'escalation' THEN 0
                    WHEN cm.role = 'template' THEN 1
                    WHEN cm.event_type = 'SEND' THEN 2
                    ELSE 3
                END
        ) AS messages
    FROM combined_msgs cm
    GROUP BY 1,2
)

/* Debug: check row counts at each step
SELECT 'subscribers_scoped' AS step, COUNT(*) AS cnt FROM subscribers_scoped
UNION ALL SELECT 'conversation_json', COUNT(*) FROM conversation_json
UNION ALL SELECT 'cqa_send_bounds', COUNT(*) FROM cqa_send_bounds
UNION ALL SELECT 'last_5_products', COUNT(*) FROM last_5_products
UNION ALL SELECT 'unified_orders', COUNT(*) FROM unified_orders
ORDER BY 2 DESC; */

SELECT
    cj.send_id,
    dc.has_shopify_ecomm_flag AS has_shopify,
    sb.company_name,
    sb.company_website,
    sb.persona,
    CASE
        WHEN sb.message_tone = 'MESSAGE_TONE_FORMAL' THEN 'Formal'
        WHEN sb.message_tone = 'MESSAGE_TONE_CASUAL' THEN 'Casual'
        WHEN sb.message_tone = 'MESSAGE_TONE_SUPER_CASUAL' THEN 'Super Casual'
        WHEN sb.message_tone = 'MESSAGE_TONE_POLISHED' THEN 'Polished'
        ELSE 'Polished'
    END AS message_tone,
    cj.messages AS conversation_json,
    l5p.last_5_products,
    uo.orders,
    COALESCE(cd.coupons, ARRAY_CONSTRUCT()) AS coupons,
    sb.company_notes,
    sb.escalation_topics,
    sb.blocklisted_words
FROM conversation_json cj
JOIN cqa_send_bounds sb
    ON sb.session_id = cj.session_id
    AND sb.send_id = cj.send_id
LEFT JOIN dw.dim_company dc
    ON dc.company_id = sb.company_id
LEFT JOIN last_5_products l5p
    ON l5p.send_id = cj.send_id
    AND l5p.user_id = sb.user_id
LEFT JOIN unified_orders uo
    ON uo.send_id = cj.send_id
    AND uo.user_id = sb.user_id
LEFT JOIN coupon_data cd
    ON cd.send_id = cj.send_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY cj.send_id ORDER BY sb.send_time_est DESC) = 1;
