package io.vepo.datamotion;

import javassist.*;
import org.apache.commons.text.CaseUtils;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        try {
            ClassPool cp = new ClassPool(false);
            cp.appendSystemPath();
            CtClass clz = cp.makeClass(config.getMainClassFullQualifiedName());
            CtField f = new CtField(CtClass.intType, "intValue", clz);
            f.setModifiers(Modifier.PRIVATE);
            clz.addField(f);
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

    public static void main(String[] args) {

    }
}
