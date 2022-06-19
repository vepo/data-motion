package io.vepo.datamotion;

import com.soebes.itf.jupiter.extension.*;
import com.soebes.itf.jupiter.maven.MavenExecutionResult;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.soebes.itf.extension.assertj.MavenITAssertions.assertThat;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@MavenJupiterExtension
public class HelloWorldIT {
    @MavenTest
    void simple(MavenExecutionResult result) {
        System.out.println(result);
        assertThat(result).isSuccessful()
                          .project()
                          .hasTarget()
                          .withJarFile()
                          .containsOnlyOnce("io/vepo/streamer/HelloWorldStreamer.class",
                                            "META-INF/MANIFEST.MF")
                          .is(new Condition<File>(jarFile -> {
                              try {
                                  Process proc = Runtime.getRuntime().exec(String.format("java -jar \"%s\"", jarFile.getAbsolutePath()));
                                  if (proc.waitFor(1, TimeUnit.SECONDS) &&
                                          proc.exitValue() == 0) {
                                      String stdOut = new BufferedReader(new InputStreamReader(proc.getInputStream())).lines()
                                                                                                                      .parallel()
                                                                                                                      .collect(joining("\n"));
                                      assertThat(stdOut).isEqualTo("Hello World!");
                                      return true;
                                  } else {
                                      return false;
                                  }
                              } catch (InterruptedException | IOException e) {
                                  fail("Jar is not executable!", e);
                                  return false;
                              }
                          }, "Can be executed!"));
    }

}
