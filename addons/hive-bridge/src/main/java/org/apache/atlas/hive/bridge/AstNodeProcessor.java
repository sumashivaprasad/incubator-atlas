package org.apache.atlas.hive.bridge;


import org.apache.hadoop.hive.ql.parse.ASTNode;

public interface ASTNodeProcessor {
    void process(RewriteContext ctx, ASTNode node) throws RewriteException;
}
