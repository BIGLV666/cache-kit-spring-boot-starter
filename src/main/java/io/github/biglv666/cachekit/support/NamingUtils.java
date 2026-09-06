package io.github.biglv666.cachekit.support;

/**
 * 命名转换工具：默认键前缀策略与 MyBatis 的 map-underscore-to-camel-case 约定互逆。
 */
public final class NamingUtils {

    private NamingUtils() {
    }

    /**
     * 驼峰转蛇形：{@code UserLogin} → {@code user_login}，{@code userID} → {@code user_i_d} 的
     * 极端大写缩写场景按逐字符处理，仅作为键前缀使用，不影响业务列名。
     *
     * @param name 驼峰命名（通常是类名）
     * @return 蛇形命名（全小写下划线分隔）
     */
    public static String camelToSnake(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        StringBuilder sb = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
