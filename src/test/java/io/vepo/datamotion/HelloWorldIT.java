package io.vepo.datamotion;

import com.soebes.itf.jupiter.extension.MavenJupiterExtension;
import com.soebes.itf.jupiter.extension.MavenTest;
import com.soebes.itf.jupiter.maven.MavenExecutionResult;

import static com.soebes.itf.extension.assertj.MavenITAssertions.assertThat;

@MavenJupiterExtension
public class HelloWorldIT {
    @MavenTest
    void simple(MavenExecutionResult result) {
        System.out.println(result);
        assertThat(result).isSuccessful();
    }

}
