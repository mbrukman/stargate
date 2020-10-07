/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.util;

public class Naming {

  private Naming() {}

  /**
   * Converts the name of a CQL schema element (keyspace, table, column...) into the corresponding
   * GraphQL identifier.
   *
   * @param cqlName the CQL name, in its "internal" form: exact case and unquoted (how it appears in
   *     system tables, and is surfaced by the persistence API).
   */
  public static String toGraphQl(String cqlName) {
    if (cqlName == null || cqlName.isEmpty()) {
      throw new IllegalArgumentException("CQL name must be non-null and not empty");
    }
    StringBuilder out = new StringBuilder();
    int length = cqlName.length();
    for (int i = 0; i < length; i++) {
      char current = cqlName.charAt(i);
      // Special case: encode surrogate pairs as a single entity
      if (i < length - 1 && Character.isSurrogatePair(current, cqlName.charAt(i + 1))) {
        appendSpecialChar(Character.codePointAt(cqlName, i), out);
        // Note: we only need to increment when we consumed additional characters, the loop already
        // increments once.
        i += 1;
      } else if (isSpecialChar(current, i)) {
        appendSpecialChar(Character.codePointAt(cqlName, i), out);
      } else if (current == '_') {
        if (length == 1) {
          // Input string is "_": encode to avoid empty string.
          appendSpecialChar(Character.codePointAt(cqlName, i), out);
        } else if (i < length - 1) {
          char next = cqlName.charAt(i + 1);
          if (next == '_') {
            // Two consecutive underscores: keep the second one (encoded), so that "a__a" encodes to
            // something different than "a_a". This also covers the input string "__".
            appendSpecialChar(Character.codePointAt(cqlName, i), out);
            i += 1;
          } else if (isLowerAlpha(next)) {
            // Uppercase the next char (camel case).
            out.append(Character.toUpperCase(next));
            i += 1;
          }
        }
      } else {
        out.append(current);
      }
    }
    return out.toString();
  }

  private static boolean isSpecialChar(char c, int position) {
    return (position == 0 && isDigit(c))
        || !(isDigit(c) || isLowerAlpha(c) || isUpperAlpha(c) || c == '_');
  }

  private static boolean isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  private static boolean isLowerAlpha(char c) {
    return c >= 'a' && c <= 'z';
  }

  private static boolean isUpperAlpha(char c) {
    return c >= 'A' && c <= 'Z';
  }

  private static void appendSpecialChar(int codePoint, StringBuilder out) {
    out.append('x').append(Integer.toHexString(codePoint)).append('_');
  }
}
