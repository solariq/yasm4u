package ru.yandex.se.lyadzhin.report;

import ru.yandex.se.yasm4u.domains.wb.StateRef;

/**
 * User: lyadzhin
 * Date: 13.04.15 10:19
 */
public final class SearchWhiteboard {

  public static final StateRef<String> HTTP_REQ_HOST = new StateRef<>("http_req_referrer", String.class);
  public static final StateRef<String> HTTP_REQ_REFERRER = new StateRef<>("http_req_referrer", String.class);
  public static final StateRef<String> HTTP_REQ_USER_AGENT = new StateRef<>("http_user_agent", String.class);

  public static final StateRef<Void> USER_IP_ADDRESS = new StateRef<>("user_ip_addres", Void.class);
  public static final StateRef<String> USER_YANDEX_UID = new StateRef<>("user_yandex_uid", String.class);
  public static final StateRef<Void> USER_YA_PASSPORT = new StateRef<>("user_ya_passport", Void.class);
  public static final StateRef<Void> USER_PREFERENCES = new StateRef<>("user_preferences", Void.class);

  public static final StateRef<String> USER_QUERY_TEXT = new StateRef<>("user_query_text", String.class);

  private SearchWhiteboard() {}
}
