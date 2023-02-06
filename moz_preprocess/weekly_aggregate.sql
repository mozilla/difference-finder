/*
This query joins a week's worth of data from Clients Daily to Search Clients Daily on (client_id, submission_date).
Then, it aggregates the joined data for each client over the entire week. 
*/

--Unpack event counts by type
CREATE TEMP FUNCTION
  count_event_by_type(event_type STRING,
    scalar_parent_telemetry_event_counts_sum ANY type) AS ( (
    SELECT
      SUM(
      IF
        (COALESCE(SPLIT(x.key,"#")[
          OFFSET
            (0)] = event_type, FALSE), x.value, 0))
    FROM
      UNNEST(scalar_parent_telemetry_event_counts_sum) AS x)) ;
WITH
  search_clients AS (
  SELECT
    client_id,
    submission_date,
    --Custom sums.
    SUM(
    IF
      (LOWER(SOURCE) LIKE "%urlbar%", COALESCE(sap, 0), 0)) AS urlbar_searches,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(organic, 0), 0)) AS google_organic,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(tagged_sap, 0), 0)) AS google_tagged_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(tagged_follow_on, 0), 0)) AS google_tagged_follow_on,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(sap, 0), 0)) AS google_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(ad_click, 0), 0)) AS google_ad_click,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(ad_click_organic, 0), 0)) AS google_ad_click_organic,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(search_with_ads, 0), 0)) AS google_search_with_ads,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%", COALESCE(search_with_ads_organic, 0), 0)) AS google_search_with_ads_organic,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(organic, 0), 0)) AS bing_organic,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(tagged_sap, 0), 0)) AS bing_tagged_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(tagged_follow_on, 0), 0)) AS bing_tagged_follow_on,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(sap, 0), 0)) AS bing_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(ad_click, 0), 0)) AS bing_ad_click,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(ad_click_organic, 0), 0)) AS bing_ad_click_organic,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(search_with_ads, 0), 0)) AS bing_search_with_ads,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%", COALESCE(search_with_ads_organic, 0), 0)) AS bing_search_with_ads_organic,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(organic, 0), 0)) AS ddg_organic,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(tagged_sap, 0), 0)) AS ddg_tagged_sap,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(tagged_follow_on, 0), 0)) AS ddg_tagged_follow_on,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(sap, 0), 0)) AS ddg_sap,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(ad_click, 0), 0)) AS ddg_ad_click,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(ad_click_organic, 0), 0)) AS ddg_ad_click_organic,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(search_with_ads, 0), 0)) AS ddg_search_with_ads,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg"), COALESCE(search_with_ads_organic, 0), 0)) AS ddg_search_with_ads_organic,
    /* These metrics are not yet implemented.
    SUM(
    IF
      (LOWER(engine) LIKE "%google%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_sap, 0), 0)) AS google_monetizable_tagged_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_follow_on, 0), 0)) AS google_monetizable_tagged_follow_on,
    SUM(
    IF
      (LOWER(engine) LIKE "%google%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(sap, 0), 0)) AS google_monetizable_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_sap, 0), 0)) AS bing_monetizable_tagged_sap,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_follow_on, 0), 0)) AS bing_monetizable_tagged_follow_on,
    SUM(
    IF
      (LOWER(engine) LIKE "%bing%"
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(sap, 0), 0)) AS bing_monetizable_sap,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg")
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_sap, 0), 0)) AS ddg_monetizable_tagged_sap,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg")
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(tagged_follow_on, 0), 0)) AS ddg_monetizable_tagged_follow_on,
    SUM(
    IF
      (REGEXP_CONTAINS(LOWER(engine), "duckduckgo|ddg")
        AND is_sap_monetizable
        AND NOT has_adblocker_addon, COALESCE(sap, 0), 0)) AS ddg_monetizable_sap,
    */
    --Aggregate by summing.
    SUM(COALESCE(organic, 0)) AS organic,
    SUM(COALESCE(tagged_sap, 0)) AS tagged_sap,
    SUM(COALESCE(tagged_follow_on, 0)) AS tagged_follow_on,
    SUM(COALESCE(sap, 0)) AS sap,
    SUM(COALESCE(ad_click, 0)) AS ad_click,
    SUM(COALESCE(ad_click_organic, 0)) AS ad_click_organic,
    SUM(COALESCE(search_with_ads, 0)) AS search_with_ads,
    SUM(COALESCE(search_with_ads_organic, 0)) AS search_with_ads_organic,
    /* These metrics are not yet implemented.
    --Aggregate by taking any value. They are all the same.
    ANY_VALUE(has_adblocker_addon) AS has_adblocker_addon,
    */
  FROM
    `mozdata.search.search_clients_engines_sources_daily`
  WHERE
    submission_date >= '{week_start_date}'
    AND submission_date <= DATE_ADD(DATE '{week_start_date}', INTERVAL 6 DAY)
  GROUP BY
    client_id,
    submission_date ),
  clients AS (
  SELECT
    * EXCEPT(search_with_ads),
    --Sum event types separately.
    count_event_by_type("doh",
      scalar_parent_telemetry_event_counts_sum) AS event_doh,
    count_event_by_type("urlbar",
      scalar_parent_telemetry_event_counts_sum) AS event_urlbar,
    count_event_by_type("navigation",
      scalar_parent_telemetry_event_counts_sum) AS event_navigation,
    count_event_by_type("pwmgr",
      scalar_parent_telemetry_event_counts_sum) AS event_pwmgr,
    count_event_by_type("addonsmanager",
      scalar_parent_telemetry_event_counts_sum) AS event_addonsmanager,
    count_event_by_type("networkdns",
      scalar_parent_telemetry_event_counts_sum) AS event_networkdns,
    count_event_by_type("downloads",
      scalar_parent_telemetry_event_counts_sum) AS event_downloads,
    count_event_by_type("devtoolsmain",
      scalar_parent_telemetry_event_counts_sum) AS event_devtoolsmain,
    count_event_by_type("form_autocomplete",
      scalar_parent_telemetry_event_counts_sum) AS event_form_autocomplete,
    count_event_by_type("close_tab_warning",
      scalar_parent_telemetry_event_counts_sum) AS event_close_tab_warning,
    count_event_by_type("pictureinpicture",
      scalar_parent_telemetry_event_counts_sum) AS event_pictureinpicture,
    count_event_by_type("slow_script_warning",
      scalar_parent_telemetry_event_counts_sum) AS event_slow_script_warning,
  FROM
    `mozdata.telemetry.clients_daily_v6`
  WHERE
    submission_date >= '{week_start_date}'
    AND submission_date <= DATE_ADD(DATE '{week_start_date}', INTERVAL 6 DAY) ),
  client_days AS (
  SELECT
    *,
    {segment} AS segment,
  FROM
    clients
  LEFT JOIN
    search_clients
  USING
    ( client_id,
      submission_date )
  WHERE
    {target} ),
clients_weekly AS (
SELECT
  client_id,
  segment,
  COUNT(DISTINCT client_id) OVER (PARTITION BY segment) as clients_per_segment,
  --Aggregate by summing.
  /* These metrics are not yet implemented
  SUM(COALESCE(CAST(has_adblocker_addon AS INT), 0)) AS days_with_addblocker_addon,
  SUM(COALESCE(google_monetizable_tagged_sap, 0)) AS google_monetizable_tagged_sap,
  SUM(COALESCE(google_monetizable_tagged_follow_on, 0)) AS google_monetizable_tagged_follow_on,
  SUM(COALESCE(google_monetizable_sap, 0)) AS google_monetizable_sap,
  SUM(COALESCE(bing_monetizable_tagged_sap, 0)) AS bing_monetizable_tagged_sap,
  SUM(COALESCE(bing_monetizable_tagged_follow_on, 0)) AS bing_monetizable_tagged_follow_on,
  SUM(COALESCE(bing_monetizable_sap, 0)) AS bing_monetizable_sap,
  SUM(COALESCE(ddg_monetizable_tagged_sap, 0)) AS ddg_monetizable_tagged_sap,
  SUM(COALESCE(ddg_monetizable_tagged_follow_on, 0)) AS ddg_monetizable_tagged_follow_on,
  SUM(COALESCE(ddg_monetizable_sap, 0)) AS ddg_monetizable_sap,
  */ SUM(COALESCE(organic, 0)) AS organic,
  SUM(COALESCE(tagged_sap, 0)) AS tagged_sap,
  SUM(COALESCE(tagged_follow_on, 0)) AS tagged_follow_on,
  SUM(COALESCE(sap, 0)) AS sap,
  SUM(COALESCE(ad_click, 0)) AS ad_click,
  SUM(COALESCE(ad_click_organic, 0)) AS ad_click_organic,
  SUM(COALESCE(search_with_ads, 0)) AS search_with_ads,
  SUM(COALESCE(search_with_ads_organic, 0)) AS search_with_ads_organic,
  SUM(COALESCE(urlbar_searches, 0)) AS urlbar_searches,
  SUM(COALESCE(google_organic, 0)) AS google_organic,
  SUM(COALESCE(google_tagged_sap, 0)) AS google_tagged_sap,
  SUM(COALESCE(google_tagged_follow_on, 0)) AS google_tagged_follow_on,
  SUM(COALESCE(google_sap, 0)) AS google_sap,
  SUM(COALESCE(google_ad_click, 0)) AS google_ad_click,
  SUM(COALESCE(google_ad_click_organic, 0)) AS google_ad_click_organic,
  SUM(COALESCE(google_search_with_ads, 0)) AS google_search_with_ads,
  SUM(COALESCE(google_search_with_ads_organic, 0)) AS google_search_with_ads_organic,
  SUM(COALESCE(bing_organic, 0)) AS bing_organic,
  SUM(COALESCE(bing_tagged_sap, 0)) AS bing_tagged_sap,
  SUM(COALESCE(bing_tagged_follow_on, 0)) AS bing_tagged_follow_on,
  SUM(COALESCE(bing_sap, 0)) AS bing_sap,
  SUM(COALESCE(bing_ad_click, 0)) AS bing_ad_click,
  SUM(COALESCE(bing_ad_click_organic, 0)) AS bing_ad_click_organic,
  SUM(COALESCE(bing_search_with_ads, 0)) AS bing_search_with_ads,
  SUM(COALESCE(bing_search_with_ads_organic, 0)) AS bing_search_with_ads_organic,
  SUM(COALESCE(ddg_organic, 0)) AS ddg_organic,
  SUM(COALESCE(ddg_tagged_sap, 0)) AS ddg_tagged_sap,
  SUM(COALESCE(ddg_tagged_follow_on, 0)) AS ddg_tagged_follow_on,
  SUM(COALESCE(ddg_sap, 0)) AS ddg_sap,
  SUM(COALESCE(ddg_ad_click, 0)) AS ddg_ad_click,
  SUM(COALESCE(ddg_ad_click_organic, 0)) AS ddg_ad_click_organic,
  SUM(COALESCE(ddg_search_with_ads, 0)) AS ddg_search_with_ads,
  SUM(COALESCE(ddg_search_with_ads_organic, 0)) AS ddg_search_with_ads_organic,
  SUM(COALESCE(event_doh, 0)) AS event_doh,
  SUM(COALESCE(event_urlbar, 0)) AS event_urlbar,
  SUM(COALESCE(event_navigation, 0)) AS event_navigation,
  SUM(COALESCE(event_pwmgr, 0)) AS event_pwmgr,
  SUM(COALESCE(event_addonsmanager, 0)) AS event_addonsmanager,
  SUM(COALESCE(event_networkdns, 0)) AS event_networkdns,
  SUM(COALESCE(event_downloads, 0)) AS event_downloads,
  SUM(COALESCE(event_devtoolsmain, 0)) AS event_devtoolsmain,
  SUM(COALESCE(event_form_autocomplete, 0)) AS event_form_autocomplete,
  SUM(COALESCE(event_close_tab_warning, 0)) AS event_close_tab_warning,
  SUM(COALESCE(event_pictureinpicture, 0)) AS event_pictureinpicture,
  SUM(COALESCE(event_slow_script_warning, 0)) AS event_slow_script_warning,
  SUM(COALESCE(aborts_content_sum, 0)) AS aborts_content_sum,
  SUM(COALESCE(aborts_gmplugin_sum, 0)) AS aborts_gmplugin_sum,
  SUM(COALESCE(aborts_plugin_sum, 0)) AS aborts_plugin_sum,
  SUM(COALESCE(active_hours_sum, 0)) AS active_hours_sum,
  SUM(COALESCE(crashes_detected_content_sum, 0)) AS crashes_detected_content_sum,
  SUM(COALESCE(crashes_detected_gmplugin_sum, 0)) AS crashes_detected_gmplugin_sum,
  SUM(COALESCE(crashes_detected_plugin_sum, 0)) AS crashes_detected_plugin_sum,
  SUM(COALESCE(crash_submit_attempt_content_sum, 0)) AS crash_submit_attempt_content_sum,
  SUM(COALESCE(crash_submit_attempt_main_sum, 0)) AS crash_submit_attempt_main_sum,
  SUM(COALESCE(crash_submit_attempt_plugin_sum, 0)) AS crash_submit_attempt_plugin_sum,
  SUM(COALESCE(crash_submit_success_content_sum, 0)) AS crash_submit_success_content_sum,
  SUM(COALESCE(crash_submit_success_main_sum, 0)) AS crash_submit_success_main_sum,
  SUM(COALESCE(crash_submit_success_plugin_sum, 0)) AS crash_submit_success_plugin_sum,
  SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS pings_aggregated_by_this_row,
  SUM(COALESCE(plugin_hangs_sum, 0)) AS plugin_hangs_sum,
  SUM(COALESCE(plugins_infobar_allow_sum, 0)) AS plugins_infobar_allow_sum,
  SUM(COALESCE(plugins_infobar_block_sum, 0)) AS plugins_infobar_block_sum,
  SUM(COALESCE(plugins_infobar_shown_sum, 0)) AS plugins_infobar_shown_sum,
  SUM(COALESCE(plugins_notification_shown_sum, 0)) AS plugins_notification_shown_sum,
  SUM(COALESCE(push_api_notify_sum, 0)) AS push_api_notify_sum,
  SUM(COALESCE(scalar_combined_webrtc_nicer_stun_retransmits_sum, 0)) AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
  SUM(COALESCE(scalar_combined_webrtc_nicer_turn_401s_sum, 0)) AS scalar_combined_webrtc_nicer_turn_401s_sum,
  SUM(COALESCE(scalar_combined_webrtc_nicer_turn_403s_sum, 0)) AS scalar_combined_webrtc_nicer_turn_403s_sum,
  SUM(COALESCE(scalar_combined_webrtc_nicer_turn_438s_sum, 0)) AS scalar_combined_webrtc_nicer_turn_438s_sum,
  SUM(COALESCE(scalar_content_navigator_storage_estimate_count_sum, 0)) AS scalar_content_navigator_storage_estimate_count_sum,
  SUM(COALESCE(scalar_content_navigator_storage_persist_count_sum, 0)) AS scalar_content_navigator_storage_persist_count_sum,
  SUM(COALESCE(scalar_parent_browser_engagement_tab_open_event_count_sum, 0)) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(COALESCE(scalar_parent_browser_engagement_total_uri_count_sum, 0)) AS scalar_parent_browser_engagement_total_uri_count_sum,
  SUM(COALESCE(scalar_parent_browser_engagement_unfiltered_uri_count_sum, 0)) AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
  SUM(COALESCE(scalar_parent_browser_engagement_window_open_event_count_sum, 0)) AS scalar_parent_browser_engagement_window_open_event_count_sum,
  SUM(COALESCE(scalar_parent_devtools_accessibility_node_inspected_count_sum, 0)) AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
  SUM(COALESCE(scalar_parent_devtools_accessibility_opened_count_sum, 0)) AS scalar_parent_devtools_accessibility_opened_count_sum,
  SUM(COALESCE(scalar_parent_devtools_accessibility_picker_used_count_sum, 0)) AS scalar_parent_devtools_accessibility_picker_used_count_sum,
  SUM(COALESCE(scalar_parent_devtools_accessibility_service_enabled_count_sum, 0)) AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
  SUM(COALESCE(scalar_parent_devtools_copy_full_css_selector_opened_sum, 0)) AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
  SUM(COALESCE(scalar_parent_devtools_copy_unique_css_selector_opened_sum, 0)) AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
  SUM(COALESCE(scalar_parent_devtools_toolbar_eyedropper_opened_sum, 0)) AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
  SUM(COALESCE(scalar_parent_navigator_storage_estimate_count_sum, 0)) AS scalar_parent_navigator_storage_estimate_count_sum,
  SUM(COALESCE(scalar_parent_navigator_storage_persist_count_sum, 0)) AS scalar_parent_navigator_storage_persist_count_sum,
  SUM(COALESCE(scalar_parent_storage_sync_api_usage_extensions_using_sum, 0)) AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
  SUM(COALESCE(shutdown_kill_sum, 0)) AS shutdown_kill_sum,
  SUM(COALESCE(subsession_hours_sum, 0)) AS subsession_hours_sum,
  SUM(COALESCE(ssl_handshake_result_failure_sum, 0)) AS ssl_handshake_result_failure_sum,
  SUM(COALESCE(ssl_handshake_result_success_sum, 0)) AS ssl_handshake_result_success_sum,
  SUM(COALESCE(sync_count_desktop_sum, 0)) AS sync_count_desktop_sum,
  SUM(COALESCE(sync_count_mobile_sum, 0)) AS sync_count_mobile_sum,
  SUM(COALESCE(web_notification_shown_sum, 0)) AS web_notification_shown_sum,
  SUM(COALESCE(trackers_blocked_sum, 0)) AS trackers_blocked_sum,
  SUM(COALESCE(scalar_parent_urlbar_impression_autofill_origin_sum, 0)) AS scalar_parent_urlbar_impression_autofill_origin_sum,
  SUM(COALESCE(n_logged_event, 0)) AS n_logged_event,
  SUM(COALESCE(n_created_pictureinpicture, 0)) AS n_created_pictureinpicture,
  SUM(COALESCE(n_viewed_protection_report, 0)) AS n_viewed_protection_report,
  SUM(COALESCE(scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum, 0)) AS scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
  SUM(COALESCE(scalar_parent_browser_ui_interaction_textrecognition_error_sum, 0)) AS scalar_parent_browser_ui_interaction_textrecognition_error_sum,
  SUM(COALESCE(text_recognition_interaction_timing_sum, 0)) AS text_recognition_interaction_timing_sum,
  SUM(COALESCE(text_recognition_interaction_timing_count_sum, 0)) AS text_recognition_interaction_timing_count_sum,
  SUM(COALESCE(text_recognition_api_performance_sum, 0)) AS text_recognition_api_performance_sum,
  SUM(COALESCE(text_recognition_api_performance_count_sum, 0)) AS text_recognition_api_performance_count_sum,
  SUM(COALESCE(text_recognition_text_length_sum, 0)) AS text_recognition_text_length_sum,
  SUM(COALESCE(text_recognition_text_length_count_sum, 0)) AS text_recognition_text_length_count_sum,
  SUM(COALESCE(main_crash_count, 0)) AS main_crash_count,
  SUM(COALESCE(content_crash_count, 0)) AS content_crash_count,
  SUM(COALESCE(gpu_crash_count, 0)) AS gpu_crash_count,
  SUM(COALESCE(rdd_crash_count, 0)) AS rdd_crash_count,
  SUM(COALESCE(socket_crash_count, 0)) AS socket_crash_count,
  SUM(COALESCE(utility_crash_count, 0)) AS utility_crash_count,
  SUM(COALESCE(total_uri_count, 0)) AS total_uri_count,
  SUM(COALESCE(total_uri_count_normal_mode, 0)) AS total_uri_count_normal_mode,
  SUM(COALESCE(total_uri_count_private_mode, 0)) AS total_uri_count_private_mode,
  SUM(COALESCE(contextual_services_topsites_click_sum, 0)) AS contextual_services_topsites_click_sum,
  SUM(COALESCE(contextual_services_topsites_impression_sum, 0)) AS contextual_services_topsites_impression_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_autofill_sum, 0)) AS scalar_parent_urlbar_picked_autofill_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_bookmark_sum, 0)) AS scalar_parent_urlbar_picked_bookmark_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_dynamic_sum, 0)) AS scalar_parent_urlbar_picked_dynamic_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_extension_sum, 0)) AS scalar_parent_urlbar_picked_extension_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_formhistory_sum, 0)) AS scalar_parent_urlbar_picked_formhistory_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_history_sum, 0)) AS scalar_parent_urlbar_picked_history_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_keyword_sum, 0)) AS scalar_parent_urlbar_picked_keyword_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_remotetab_sum, 0)) AS scalar_parent_urlbar_picked_remotetab_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_searchengine_sum, 0)) AS scalar_parent_urlbar_picked_searchengine_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_searchsuggestion_sum, 0)) AS scalar_parent_urlbar_picked_searchsuggestion_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_switchtab_sum, 0)) AS scalar_parent_urlbar_picked_switchtab_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_tabtosearch_sum, 0)) AS scalar_parent_urlbar_picked_tabtosearch_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_tip_sum, 0)) AS scalar_parent_urlbar_picked_tip_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_topsite_sum, 0)) AS scalar_parent_urlbar_picked_topsite_sum,
  SUM(COALESCE(scalar_parent_urlbar_picked_visiturl_sum, 0)) AS scalar_parent_urlbar_picked_visiturl_sum,
  SUM(COALESCE(contextual_services_quicksuggest_click_sponsored_sum, 0)) AS contextual_services_quicksuggest_click_sponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_click_nonsponsored_sum, 0)) AS contextual_services_quicksuggest_click_nonsponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_impression_sponsored_sum, 0)) AS contextual_services_quicksuggest_impression_sponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_impression_nonsponsored_sum, 0)) AS contextual_services_quicksuggest_impression_nonsponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_help_sponsored_sum, 0)) AS contextual_services_quicksuggest_help_sponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_help_nonsponsored_sum, 0)) AS contextual_services_quicksuggest_help_nonsponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_block_sponsored_sum, 0)) AS contextual_services_quicksuggest_block_sponsored_sum,
  SUM(COALESCE(contextual_services_quicksuggest_block_nonsponsored_sum, 0)) AS contextual_services_quicksuggest_block_nonsponsored_sum,
  --Aggregate means.
  SUM(COALESCE(active_addons_count_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS active_addons_count_mean,
  SUM(COALESCE(client_clock_skew_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS client_clock_skew_mean,
  SUM(COALESCE(client_submission_latency_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS client_submission_latency_mean,
  SUM(COALESCE(first_paint_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS first_paint_mean,
  SUM(COALESCE(places_bookmarks_count_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS places_bookmarks_count_mean,
  SUM(COALESCE(places_pages_count_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS places_pages_count_mean,
  SUM(COALESCE(scalar_parent_browser_engagement_unique_domains_count_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS scalar_parent_browser_engagement_unique_domains_count_mean,
  SUM(COALESCE(session_restored_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS session_restored_mean,
  SUM(COALESCE(sync_count_desktop_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS sync_count_desktop_mean,
  SUM(COALESCE(sync_count_mobile_mean * pings_aggregated_by_this_row, 0)) / SUM(COALESCE(pings_aggregated_by_this_row, 0)) AS sync_count_mobile_mean,
  --Aggregate by taking the mean.
  AVG(COALESCE(profile_age_in_days, 0)) AS profile_age_in_days,
  --Aggregate by taking the maximum.
  MAX(COALESCE(scalar_parent_browser_engagement_max_concurrent_tab_count_max, 0)) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
  MAX(COALESCE(scalar_parent_browser_engagement_max_concurrent_window_count_max, 0)) AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
  MAX(COALESCE(scalar_parent_browser_engagement_unique_domains_count_max, 0)) AS scalar_parent_browser_engagement_unique_domains_count_max,
  --Aggregate by taking any value. they are all the same.
  ANY_VALUE(sample_id) AS sample_id,
  --Custom aggregations
  COUNTIF(active_hours_sum > 0
    AND scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum > 0) AS qdou,
  --Aggregate by putting elements into an array.
  ARRAY_AGG(attribution IGNORE NULLS) AS attribution,
  ARRAY_AGG(browser_version_info IGNORE NULLS) AS browser_version_info,
  --Aggregate by concatenating arrays.
  ARRAY_CONCAT_AGG(active_addons) AS active_addons,
  ARRAY_CONCAT_AGG(a11y_theme) AS a11y_theme,
  ARRAY_CONCAT_AGG(experiments) AS experiments,
  --Aggregate by summing values by key.
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(scalar_parent_browser_ui_interaction_content_context_sum)) AS scalar_parent_browser_ui_interaction_content_context_sum,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(scalar_parent_browser_ui_interaction_preferences_pane_home_sum)) AS scalar_parent_browser_ui_interaction_preferences_pane_home_sum,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(scalar_parent_devtools_accessibility_select_accessible_for_node_sum)) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
  --Aggregate by taking the mode.
  `mozfun.stats.mode_last`(ARRAY_AGG(addon_compatibility_check_enabled)) AS addon_compatibility_check_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(app_display_version)) AS app_display_version,
  `mozfun.stats.mode_last`(ARRAY_AGG(blocklist_enabled)) AS blocklist_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_cores)) AS cpu_cores,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_count)) AS cpu_count,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_family)) AS cpu_family,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_l2_cache_kb)) AS cpu_l2_cache_kb,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_l3_cache_kb)) AS cpu_l3_cache_kb,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_model)) AS cpu_model,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_speed_mhz)) AS cpu_speed_mhz,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_stepping)) AS cpu_stepping,
  `mozfun.stats.mode_last`(ARRAY_AGG(cpu_vendor)) AS cpu_vendor,
  `mozfun.stats.mode_last`(ARRAY_AGG(default_search_engine_data_name)) AS default_search_engine_data_name,
  `mozfun.stats.mode_last`(ARRAY_AGG(distribution_id)) AS distribution_id,
  `mozfun.stats.mode_last`(ARRAY_AGG(e10s_enabled)) AS e10s_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(env_build_arch)) AS env_build_arch,
  `mozfun.stats.mode_last`(ARRAY_AGG(flash_version)) AS flash_version,
  `mozfun.stats.mode_last`(ARRAY_AGG(country)) AS country,
  `mozfun.stats.mode_last`(ARRAY_AGG(city)) AS city,
  `mozfun.stats.mode_last`(ARRAY_AGG(geo_subdivision1)) AS geo_subdivision1,
  `mozfun.stats.mode_last`(ARRAY_AGG(geo_subdivision2)) AS geo_subdivision2,
  `mozfun.stats.mode_last`(ARRAY_AGG(isp_name)) AS isp_name,
  `mozfun.stats.mode_last`(ARRAY_AGG(isp_organization)) AS isp_organization,
  `mozfun.stats.mode_last`(ARRAY_AGG(gfx_features_advanced_layers_status)) AS gfx_features_advanced_layers_status,
  `mozfun.stats.mode_last`(ARRAY_AGG(gfx_features_d2d_status)) AS gfx_features_d2d_status,
  `mozfun.stats.mode_last`(ARRAY_AGG(gfx_features_d3d11_status)) AS gfx_features_d3d11_status,
  `mozfun.stats.mode_last`(ARRAY_AGG(gfx_features_gpu_process_status)) AS gfx_features_gpu_process_status,
  `mozfun.stats.mode_last`(ARRAY_AGG(install_year)) AS install_year,
  `mozfun.stats.mode_last`(ARRAY_AGG(is_default_browser)) AS is_default_browser,
  `mozfun.stats.mode_last`(ARRAY_AGG(is_wow64)) AS is_wow64,
  `mozfun.stats.mode_last`(ARRAY_AGG(locale)) AS locale,
  `mozfun.stats.mode_last`(ARRAY_AGG(memory_mb)) AS memory_mb,
  `mozfun.stats.mode_last`(ARRAY_AGG(normalized_channel)) AS normalized_channel,
  `mozfun.stats.mode_last`(ARRAY_AGG(normalized_os_version)) AS normalized_os_version,
  `mozfun.stats.mode_last`(ARRAY_AGG(os)) AS os,
  `mozfun.stats.mode_last`(ARRAY_AGG(os_version)) AS os_version,
  `mozfun.stats.mode_last`(ARRAY_AGG(sandbox_effective_content_process_level)) AS sandbox_effective_content_process_level,
  `mozfun.stats.mode_last`(ARRAY_AGG(sync_configured)) AS sync_configured,
  `mozfun.stats.mode_last`(ARRAY_AGG(telemetry_enabled)) AS telemetry_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(timezone_offset)) AS timezone_offset,
  `mozfun.stats.mode_last`(ARRAY_AGG(update_auto_download)) AS update_auto_download,
  `mozfun.stats.mode_last`(ARRAY_AGG(update_channel)) AS update_channel,
  `mozfun.stats.mode_last`(ARRAY_AGG(update_enabled)) AS update_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(vendor)) AS vendor,
  `mozfun.stats.mode_last`(ARRAY_AGG(windows_build_number)) AS windows_build_number,
  `mozfun.stats.mode_last`(ARRAY_AGG(windows_ubr)) AS windows_ubr,
  `mozfun.stats.mode_last`(ARRAY_AGG(fxa_configured)) AS fxa_configured,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_is_taskbar_pinned)) AS scalar_parent_os_environment_is_taskbar_pinned,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_desktop)) AS scalar_parent_os_environment_launched_via_desktop,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_taskbar)) AS scalar_parent_os_environment_launched_via_taskbar,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_other)) AS scalar_parent_os_environment_launched_via_other,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_start_menu)) AS scalar_parent_os_environment_launched_via_start_menu,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_other_shortcut)) AS scalar_parent_os_environment_launched_via_other_shortcut,
  `mozfun.stats.mode_last`(ARRAY_AGG(default_private_search_engine)) AS default_private_search_engine,
  `mozfun.stats.mode_last`(ARRAY_AGG(user_pref_browser_search_region)) AS user_pref_browser_search_region,
  `mozfun.stats.mode_last`(ARRAY_AGG(update_background)) AS update_background,
  `mozfun.stats.mode_last`(ARRAY_AGG(user_pref_browser_urlbar_suggest_searches)) AS user_pref_browser_urlbar_suggest_searches,
  `mozfun.stats.mode_last`(ARRAY_AGG(user_pref_browser_newtabpage_enabled)) AS user_pref_browser_newtabpage_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(user_pref_app_shield_optoutstudies_enabled)) AS user_pref_app_shield_optoutstudies_enabled,
  `mozfun.stats.mode_last`(ARRAY_AGG(scalar_parent_os_environment_launched_via_taskbar_private)) AS scalar_parent_os_environment_launched_via_taskbar_private,
  `mozfun.stats.mode_last`(ARRAY_AGG(dom_parentprocess_private_window_used)) AS dom_parentprocess_private_window_used,
  `mozfun.stats.mode_last`(ARRAY_AGG(os_environment_is_taskbar_pinned_any)) AS os_environment_is_taskbar_pinned_any,
  `mozfun.stats.mode_last`(ARRAY_AGG(os_environment_is_taskbar_pinned_private_any)) AS os_environment_is_taskbar_pinned_private_any,
  `mozfun.stats.mode_last`(ARRAY_AGG(os_environment_is_taskbar_pinned_private)) AS os_environment_is_taskbar_pinned_private,
  `mozfun.stats.mode_last`(ARRAY_AGG(search_cohort)) AS search_cohort,
  `mozfun.stats.mode_last`(ARRAY_AGG(user_pref_browser_urlbar_quicksuggest_data_collection_enabled)) AS user_pref_browser_urlbar_quicksuggest_data_collection_enabled,
FROM
  client_days
GROUP BY
  client_id,
  segment
)
SELECT
  * EXCEPT(clients_per_segment)
FROM
  clients_weekly
WHERE
  {sample}