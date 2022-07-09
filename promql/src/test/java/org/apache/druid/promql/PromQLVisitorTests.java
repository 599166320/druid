package org.apache.druid.promql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.apache.druid.promql.antlr.PromQLLexer;
import org.apache.druid.promql.antlr.PromQLParser;
import org.apache.druid.promql.logical.Operator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PromQLVisitorTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(PromQLVisitorTests.class);
    @Test
    public void test() throws JsonProcessingException {
        String promql = "aaa{id=\"1\",n=\"hello\"}/100>10+1";
        //String promql = "1+1>2";
        PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(promql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PromQLParser parser = new PromQLParser(tokenStream);
        PromQLParser.ExpressionContext expressionContext = parser.expression();
        System.out.println(expressionContext.toStringTree(parser));
        PromQLVisitor visitor = new PromQLVisitor();
        Operator result = visitor.visitExpression(expressionContext);
        log.info(MAPPER.writeValueAsString(result));
    }

}
