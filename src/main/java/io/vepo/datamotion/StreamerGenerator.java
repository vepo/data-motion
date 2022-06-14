package io.vepo.datamotion;

import javassist.*;
import org.apache.maven.project.MavenProject;

import java.io.IOException;

public class StreamerGenerator {
    private final String compiledSources;
    private final StreamerDefinition streamerDefinition;

    public StreamerGenerator(String compiledSources, StreamerDefinition streamerDefinition) {
        this.compiledSources = compiledSources;
        this.streamerDefinition = streamerDefinition;
    }

    public void generate() {
        try {
            ClassPool cp = new ClassPool(false);
            cp.appendSystemPath();
            CtClass clz = cp.makeClass(this.streamerDefinition.getPackageName() + ".Streamer");
            CtField f = new CtField(CtClass.intType, "intValue", clz);
            f.setModifiers(Modifier.PRIVATE);
            clz.addField(f);
            clz.writeFile(compiledSources);
            System.out.println("Writing to: " + compiledSources);
        } catch (CannotCompileException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamerGenerator from(MavenProject project, StreamerDefinition streamerDefinition) {
        return new StreamerGenerator(project.getCompileSourceRoots().get(0
        ), streamerDefinition);
    }
}
