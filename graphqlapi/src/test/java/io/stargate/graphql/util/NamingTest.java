package io.stargate.graphql.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NamingTest {

  @ParameterizedTest
  @MethodSource("toGraphQlParameters")
  @DisplayName("Should convert CQL identifier to GraphQL")
  public void toGraphQlTest(String cql, String expectedGraphQl) {
    assertThat(Naming.toGraphQl(cql)).isEqualTo(expectedGraphQl);
  }

  public static Stream<Arguments> toGraphQlParameters() {
    return Stream.of(
        arguments("foobar", "foobar"),
        arguments("foo_bar", "fooBar"),
        arguments("fooBar", "fooBar"),
        arguments("foo__bar", "foox5f_bar"),
        arguments("123", "x31_23"),
        arguments("_", "x5f_"),
        arguments("__", "x5f_"),
        arguments("Hello world!", "Hellox20_worldx21_"),
        arguments("\uD83D\uDD77", "x1f577_"),
        arguments("ὗ7", "x1f57_7"));
  }
}
