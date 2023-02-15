package io.vepo.datamotion;

import com.soebes.itf.jupiter.extension.*;
import com.soebes.itf.jupiter.maven.MavenExecutionResult;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.soebes.itf.extension.assertj.MavenITAssertions.assertThat;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@MavenJupiterExtension
public class HelloWorldIT {
    private static final Logger logger = LoggerFactory.getLogger(HelloWorldIT.class);
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
                                  Process proc = Runtime.getRuntime().exec(new String[]{"java", "-jar", jarFile.getAbsolutePath()});
                                  if (proc.waitFor(10, TimeUnit.SECONDS) &&
                                          proc.exitValue() == 0) {
                                      String stdOut = read(proc.getInputStream());
                                      assertThat(stdOut).isEqualTo("Hello World!");
                                      return true;
                                  } else {
                                      String stdErr = read(proc.getErrorStream());
                                      System.out.println(stdErr);
                                      Process lsProc = Runtime.getRuntime().exec(new String[]{"ls", "-lah", jarFile.getParentFile().getAbsolutePath()});
                                      System.out.println("------------------");
                                      System.out.println(read(lsProc.getInputStream()));
                                      System.out.println("------------------");
                                      System.out.println(read(lsProc.getErrorStream()));
                                      System.out.println("------------------");
                                      return false;
                                  }
                              } catch (InterruptedException | IOException e) {
                                  fail("Jar is not executable!", e);
                                  return false;
                              }
                          }, "Can be executed!"));
    }

    private static final String read(InputStream is) {
        return new BufferedReader(new InputStreamReader(is)).lines()
                                                            .parallel()
                                                            .collect(joining("\n"));
    }

}
