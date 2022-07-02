package io.vepo.datamotion;

import javassist.*;
import org.apache.maven.model.Dependency;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;

public class StreamerGenerator {
    private final File classesDirectory;
    private final File generatedSourcesDirectory;
    private final StreamerConfiguration config;

    public StreamerGenerator(File classesDirectory, File generatedSourcesDirectory, StreamerConfiguration config) {
        this.classesDirectory = classesDirectory;
        this.generatedSourcesDirectory = generatedSourcesDirectory;
        this.config = config;
    }

    public void generate(MavenProject project) {
        // <!-- https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams -->
        //<dependency>
        //    <groupId>org.apache.kafka</groupId>
        //    <artifactId>kafka-streams</artifactId>
        //    <version>3.2.0</version>
        //</dependency>
        Dependency kafkaDependency = new Dependency();
        kafkaDependency.setGroupId("org.apache.kafka");
        kafkaDependency.setArtifactId("kafka-streams");
        kafkaDependency.setVersion("3.2.0");
        project.getModel().addDependency(kafkaDependency);
        try {
            ClassPool cp = new ClassPool(false);
            cp.appendSystemPath();
            CtClass clz = cp.makeClass(config.getMainClassFullQualifiedName());
            CtMethod m = new CtMethod(CtClass.voidType, "main", new CtClass[]{cp.get("java.lang.String[]")}, clz);
            m.setBody("{System.out.println(\"Hello World!\");}");
            m.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
            clz.addMethod(m);
            clz.writeFile(classesDirectory.getAbsolutePath());
            clz.detach();
        } catch (CannotCompileException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamerGenerator from(File classesDirectory, File generatedSourcesDirectory,
                                         StreamerConfiguration config) {
        return new StreamerGenerator(classesDirectory, generatedSourcesDirectory, config);
    }
}
