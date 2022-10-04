package mux41.ocean.orchestrator;

import lombok.Data;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.rowset.QueryResults;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class Papa {

    public static String VAR_NS = "http://vars#";
    public static String PARAMS_NS = "http://params#";
    public static String VAR_PREFIX = "var";
    public static String PARAMS_PREFIX = "param";

    public static Map<String, String> s = Map.of(
            VAR_PREFIX, VAR_NS,
            PARAMS_PREFIX, PARAMS_NS
    );

    public static String ss = s.entrySet().stream()
            .map(e -> String.format("prefix %s: <%s>\n", e.getKey(), e.getValue()))
            .collect(Collectors.joining());

    public static String ss2 = s.entrySet().stream()
            .map(e -> String.format("@prefix %s: <%s>.\n", e.getKey(), e.getValue()))
            .collect(Collectors.joining());

    Query query;
    Map<String, String> a = new HashMap<>();
    Map<String, String> b = new HashMap<>();

    public Papa(String query) {
        this.query = QueryFactory.create(query);
        varify();
        optionalify();
    }

    public Query varify() {
        var elementGroup = new ElementGroup();
        ((ElementGroup)query.getQueryPattern()).getElements()
                .forEach(element -> {
                    var t = ((ElementPathBlock) element).getPattern();
                    var g = new ElementPathBlock();
                    t.forEach(j -> g.addTriple(var(j)));
                    elementGroup.addElement(g);
                });
        query.setQueryPattern(elementGroup);
        return query;
    }

    private Triple var(TriplePath triple) {
        System.out.println(NodeFactory.createURI(VAR_NS+"d").getClass());;
        return new Triple(var(triple.getSubject()), triple.getPredicate(), var(triple.getObject()));
    }

    private Node var(Node subject) {
        if (subject.isURI()) {
            var uri = subject.getURI();
            var resource = ResourceFactory.createResource(uri);
            if (resource.getNameSpace().equals(VAR_NS)) {
                String varName = resource.getLocalName();
                a.put(varName, uri);
                return Var.alloc(varName);
            }
        }
        return subject;
    }

    public Query optionalify() {
        System.out.println(query);
        var elementGroup = new ElementGroup();
        ((ElementGroup)query.getQueryPattern()).getElements()
                .forEach(element -> ((ElementPathBlock) element).getPattern()
                        .forEach(triple -> {
                            var optionalBlock = new ElementPathBlock();
                            optionalBlock.addTriple(triple);
                            elementGroup.addElement(new ElementOptional(optionalBlock));
                        }));
        query.setQueryPattern(elementGroup);
        return query;
    }

    public void d(String data) {
        Model m = ModelFactory.createDefaultModel();
        m.read(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), null, "TTL");
        ResultSet q = QueryExecutionFactory.create(query, m).execSelect();
        while (q.hasNext()) {
            var g = q.next();
            var h = g.varNames();
            while (h.hasNext()) {
                String varName = h.next();
                b.put(g.get(varName).asResource().getURI(), a.get(varName));
            }
        }
        System.out.println(this);
    }

    public static void main(String[] args) {
        String query =
                Papa.ss +
                "PREFIX ex: <http://example.com/>" +
                "SELECT * WHERE \n" +
                "{\n" +
                "   var:s a  ex:person .\n" +
                "   var:s ex:sees var:t .\n" +
                "}";
        String data =
                Papa.ss2 +
                "@prefix ex: <http://example.com/> ." +
                "var:g a ex:person. " +
                "var:pp a ex:cat. ";
        System.out.println(query);
        Papa p = new Papa(query);
        System.out.println(p.query);
        p.d(data);
    }
}
