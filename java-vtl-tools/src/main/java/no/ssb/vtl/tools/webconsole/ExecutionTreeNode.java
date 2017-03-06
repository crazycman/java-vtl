package no.ssb.vtl.tools.webconsole;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import no.ssb.vtl.model.AbstractDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.operations.CalcOperation;
import no.ssb.vtl.script.operations.DropOperation;
import no.ssb.vtl.script.operations.FilterOperation;
import no.ssb.vtl.script.operations.FoldOperation;
import no.ssb.vtl.script.operations.KeepOperation;
import no.ssb.vtl.script.operations.RenameOperation;
import no.ssb.vtl.script.operations.UnfoldOperation;
import no.ssb.vtl.script.operations.join.InnerJoinOperation;
import no.ssb.vtl.script.operations.join.OuterJoinOperation;

import java.util.List;
import java.util.Map;

/**
 * Json object that represents the execution tree of a dataset.
 */
public class ExecutionTreeNode {

    private static final String STRUCTURE_TYPE = "structure";
    private static final Map<Class<?>, String> DATASET_TYPE_MAP;
    private static final Map<Component.Role, String> ROLE_TYPE_MAP;

    static {
        DATASET_TYPE_MAP = ImmutableMap.<Class<?>, String>builder()
                .put(FoldOperation.class, "fold")
                .put(UnfoldOperation.class, "unfold")
                .put(InnerJoinOperation.class, "inner-join")
                .put(OuterJoinOperation.class, "outer-join")
                .put(KeepOperation.class, "keep")
                .put(DropOperation.class, "drop")
                .put(RenameOperation.class, "rename")
                .put(FilterOperation.class, "filter")
                .put(CalcOperation.class, "expression")
                .put(Dataset.class, "get")
                .build();

        ROLE_TYPE_MAP = ImmutableMap.<Component.Role, String>builder()
                .put(Component.Role.IDENTIFIER, "identifier")
                .put(Component.Role.MEASURE, "measure")
                .put(Component.Role.ATTRIBUTE, "attribute")
                .build();
    }

    @JsonProperty
    private final List<ExecutionTreeNode> children = Lists.newArrayList();

    @JsonProperty
    private final String id;

    @JsonProperty
    private final String name;

    @JsonProperty
    private final String type;

    public ExecutionTreeNode(Dataset dataset, String name) {

        this.id = hashCode(dataset);
        this.name = name;
        this.type = DATASET_TYPE_MAP.getOrDefault(dataset.getClass(), "get");

        this.children.add(new ExecutionTreeNode(dataset.getDataStructure()));

        if (dataset instanceof AbstractDatasetOperation) {
            for (Dataset child : ((AbstractDatasetOperation) dataset).getChildren()) {
                this.children.add(new ExecutionTreeNode(child, null));
            }
        }

    }

    public ExecutionTreeNode(DataStructure structure) {

        this.id = hashCode(structure);
        this.name = null;
        this.type = STRUCTURE_TYPE;

        for (Map.Entry<String, Component> component : structure.entrySet()) {
            this.children.add(new ExecutionTreeNode(component.getValue(), component.getKey()));
        }

    }

    public ExecutionTreeNode(Component component, String key) {

        this.id = hashCode(component);
        this.name = key;
        this.type = ROLE_TYPE_MAP.get(component.getRole());

    }

    private static String hashCode(Object o) {
        return Long.toHexString(Integer.toUnsignedLong(o.hashCode()));
    }

    public String getType() {
        return type;
    }

    public List<ExecutionTreeNode> getChildren() {
        return children;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
