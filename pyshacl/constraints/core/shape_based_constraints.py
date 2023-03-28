# -*- coding: utf-8 -*-
"""
https://www.w3.org/TR/shacl/#core-components-shape
"""
from typing import Dict, List
from warnings import warn

import rdflib

from pyshacl.constraints.constraint_component import ConstraintComponent
from pyshacl.consts import (
    SH,
    SH_node,
    SH_NodeConstraintComponent,
    SH_property,
    SH_PropertyConstraintComponent,
    SH_nodesPath,
    SH_NodesPathConstraintComponent,
    SH_detail,
)
from pyshacl.errors import (
    ConstraintLoadError,
    ConstraintLoadWarning,
    ReportableRuntimeError,
    ShapeRecursionWarning,
    ValidationFailure,
)
from pyshacl.pytypes import GraphLike
from pyshacl.rdfutil import stringify_node
from pyshacl.helper.expression_helper import value_nodes_from_path
from textwrap import indent


SH_QualifiedValueCountConstraintComponent = SH.QualifiedValueConstraintComponent
SH_QualifiedMaxCountConstraintComponent = SH.QualifiedMaxCountConstraintComponent
SH_QualifiedMinCountConstraintComponent = SH.QualifiedMinCountConstraintComponent

SH_qualifiedValueShape = SH.qualifiedValueShape
SH_qualifiedValueShapesDisjoint = SH.qualifiedValueShapesDisjoint
SH_qualifiedMinCount = SH.qualifiedMinCount
SH_qualifiedMaxCount = SH.qualifiedMaxCount


class PropertyConstraintComponent(ConstraintComponent):
    """
    sh:property can be used to specify that each value node has a given property shape.
    Link:
    https://www.w3.org/TR/shacl/#PropertyConstraintComponent
    Textual Definition:
    For each value node v: A failure MUST be produced if the validation of v as focus node against the property shape $property produces a failure. Otherwise, the validation results are the results of validating v as focus node against the property shape $property.
    """

    shacl_constraint_component = SH_PropertyConstraintComponent
    shape_expecting = True
    list_taking = False

    def __init__(self, shape):
        super(PropertyConstraintComponent, self).__init__(shape)
        property_shapes = list(self.shape.objects(SH_property))
        if len(property_shapes) < 1:
            raise ConstraintLoadError(
                "PropertyConstraintComponent must have at least one sh:property predicate.",
                "https://www.w3.org/TR/shacl/#PropertyConstraintComponent",
            )
        self.property_shapes = property_shapes

    @classmethod
    def constraint_parameters(cls):
        return [SH_property]

    @classmethod
    def constraint_name(cls):
        return "PropertyConstraintComponent"

    def make_generic_messages(self, datagraph: GraphLike, focus_node, value_node) -> List[rdflib.Literal]:
        raise NotImplementedError("A Property Constraint Component should not be able to generate its own message.")

    def evaluate(self, target_graph: GraphLike, focus_value_nodes: Dict, _evaluation_path: List):
        """

        :type focus_value_nodes: dict
        :type target_graph: rdflib.Graph
        :type _evaluation_path: list
        """
        reports: List[Dict] = []
        non_conformant = False
        shape = self.shape

        # Shortcut, when there are no value nodes, don't check for recursion, don't validate and exit early
        value_node_count = 0
        for f, value_nodes in focus_value_nodes.items():
            value_node_count = value_node_count + len(value_nodes)
        if value_node_count < 1:
            return (not non_conformant), reports

        potentially_recursive = self.recursion_triggers(_evaluation_path)

        def _evaluate_property_shape(prop_shape):
            nonlocal shape, target_graph, focus_value_nodes, _evaluation_path, potentially_recursive
            _reports = []
            _non_conformant = False
            prop_shape = shape.get_other_shape(prop_shape)
            if potentially_recursive and prop_shape in potentially_recursive:
                warn(ShapeRecursionWarning(_evaluation_path))
                return _non_conformant, _reports
            if not prop_shape or not prop_shape.is_property_shape:
                raise ReportableRuntimeError(
                    "Shape pointed to by sh:property does not exist or is not a well-formed SHACL PropertyShape."
                )

            for f, value_nodes in focus_value_nodes.items():
                for v in value_nodes:
                    _is_conform, _r = prop_shape.validate(target_graph, focus=v, _evaluation_path=_evaluation_path[:])
                    _non_conformant = _non_conformant or (not _is_conform)
                    _reports.extend(_r)
            return _non_conformant, _reports

        for p_shape in self.property_shapes:
            _nc, _r = _evaluate_property_shape(p_shape)
            non_conformant = non_conformant or _nc
            reports.extend(_r)
        return (not non_conformant), reports


class NodeConstraintComponent(ConstraintComponent):
    """
    sh:node specifies the condition that each value node conforms to the given node shape.
    Link:
    https://www.w3.org/TR/shacl/#NodeShapeComponent
    Textual Definition:
    For each value node v: A failure MUST be produced if the conformance checking of v against $node produces a failure. Otherwise, if v does not conform to $node, there is a validation result with v as sh:value.
    """

    shacl_constraint_component = SH_NodeConstraintComponent
    shape_expecting = True
    list_taking = False

    def __init__(self, shape):
        super(NodeConstraintComponent, self).__init__(shape)
        node_shapes = list(self.shape.objects(SH_node))
        if len(node_shapes) < 1:
            raise ConstraintLoadError(
                "NodeConstraintComponent must have at least one sh:node predicate.",
                "https://www.w3.org/TR/shacl/#NodeConstraintComponent",
            )
        self.node_shapes = node_shapes

    @classmethod
    def constraint_parameters(cls):
        return [SH_node]

    @classmethod
    def constraint_name(cls):
        return "NodeConstraintComponent"

    def make_generic_messages(self, datagraph: GraphLike, focus_node, value_node) -> List[rdflib.Literal]:
        if len(self.node_shapes) < 2:
            m = "Value does not conform to Shape {}".format(stringify_node(self.shape.sg.graph, self.node_shapes[0]))
        else:
            rules = "', '".join(stringify_node(self.shape.sg.graph, c) for c in self.node_shapes)
            m = "Value does not conform to every Shape in ('{}')".format(rules)
        return [rdflib.Literal(m)]

    def evaluate(self, target_graph: GraphLike, focus_value_nodes: Dict, _evaluation_path: List):
        """

        :type focus_value_nodes: dict
        :type target_graph: rdflib.Graph
        :type _evaluation_path: list
        """
        reports: List[Dict] = []
        non_conformant = False
        shape = self.shape

        # Shortcut, when there are no value nodes, don't check for recursion, don't validate and exit early
        value_node_count = 0
        for f, value_nodes in focus_value_nodes.items():
            value_node_count = value_node_count + len(value_nodes)
        if value_node_count < 1:
            return (not non_conformant), reports

        potentially_recursive = self.recursion_triggers(_evaluation_path)

        def _evaluate_node_shape(node_shape):
            nonlocal self, target_graph, shape, focus_value_nodes, _evaluation_path, potentially_recursive
            _reports = []
            _non_conformant = False
            node_shape = shape.get_other_shape(node_shape)
            if potentially_recursive and node_shape in potentially_recursive:
                warn(ShapeRecursionWarning(_evaluation_path))
                return _non_conformant, _reports
            if not node_shape or node_shape.is_property_shape:
                raise ReportableRuntimeError(
                    "Shape pointed to by sh:node does not exist or is not a well-formed SHACL NodeShape."
                )
            for f, value_nodes in focus_value_nodes.items():
                for v in value_nodes:
                    _is_conform, _r = node_shape.validate(target_graph, focus=v, _evaluation_path=_evaluation_path[:])
                    # ignore the fails from the node, create our own fail
                    if (not _is_conform) or len(_r) > 0:
                        _non_conformant = True
                        rept = self.make_v_result(target_graph, f, value_node=v)
                        _reports.append(rept)
            return _non_conformant, _reports

        for n_shape in self.node_shapes:
            _nc, _r = _evaluate_node_shape(n_shape)
            non_conformant = non_conformant or _nc
            reports.extend(_r)
        return (not non_conformant), reports


class QualifiedValueShapeConstraintComponent(ConstraintComponent):
    """
    sh:qualifiedValueShape specifies the condition that a specified number of value nodes conforms to the given shape. Each sh:qualifiedValueShape can have: one value for sh:qualifiedMinCount, one value for sh:qualifiedMaxCount or, one value for each, at the same subject.
    Link:
    https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent
    Textual Definition for qualifiedMinCount:
    Let C be the number of value nodes v where v conforms to $qualifiedValueShape and where v does not conform to any of the sibling shapes for the current shape, i.e. the shape that v is validated against and which has $qualifiedValueShape as its value for sh:qualifiedValueShape. A failure MUST be produced if any of the said conformance checks produces a failure. Otherwise, there is a validation result if C is less than $qualifiedMinCount. The constraint component for sh:qualifiedMinCount is sh:QualifiedMinCountConstraintComponent.
    Textual Definition for qualifiedMaxCount:
    Let C be as defined for sh:qualifiedMinCount above. A failure MUST be produced if any of the said conformance checks produces a failure. Otherwise, there is a validation result if C is greater than $qualifiedMaxCount. The constraint component for sh:qualifiedMaxCount is sh:QualifiedMaxCountConstraintComponent.
    """

    shacl_constraint_component = NotImplemented
    shape_expecting = True
    list_taking = False

    def __init__(self, shape):
        super(QualifiedValueShapeConstraintComponent, self).__init__(shape)
        if not shape.is_property_shape:
            # Note, this no longer throws an error, this constraint is simply ignored on NodeShapes.
            raise ConstraintLoadWarning(
                "QualifiedValueShapeConstraintComponent can only be present on a PropertyShape, not a NodeShape.",
                "https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent",
            )
        value_shapes = set(self.shape.objects(SH_qualifiedValueShape))
        if len(value_shapes) < 1:
            raise ConstraintLoadError(
                "QualifiedValueShapeConstraintComponent must have at least one sh:qualifiedValueShape predicate.",
                "https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent",
            )
        self.value_shapes = value_shapes
        min_count = set(self.shape.objects(SH_qualifiedMinCount))
        if len(min_count) < 1:
            min_count = None
        elif len(min_count) > 1:
            raise ConstraintLoadError(
                "QualifiedMinCountConstraintComponent must have at most one sh:qualifiedMinCount predicate.",
                "https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent",
            )
        else:
            min_count = next(iter(min_count))
            assert isinstance(min_count, rdflib.Literal) and isinstance(min_count.value, int)
            min_count = min_count.value

        max_count = set(self.shape.objects(SH_qualifiedMaxCount))
        if len(max_count) < 1:
            max_count = None
        elif len(max_count) > 1:
            raise ConstraintLoadError(
                "QualifiedMaxCountConstraintComponent must have at most one sh:qualifiedMaxCount predicate.",
                "https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent",
            )
        else:
            max_count = next(iter(max_count))
            assert isinstance(max_count, rdflib.Literal) and isinstance(max_count.value, int)
            max_count = max_count.value
        if min_count is None and max_count is None:
            raise ConstraintLoadError(
                "QualifiedValueShapeConstraintComponent must have at lease one sh:qualifiedMinCount or "
                "sh:qualifiedMaxCount",
                "https://www.w3.org/TR/shacl/#QualifiedValueShapeConstraintComponent",
            )
        is_disjoint = False
        disjoint_nodes = set(self.shape.objects(SH_qualifiedValueShapesDisjoint))
        for d in disjoint_nodes:
            if isinstance(d, rdflib.Literal):
                if isinstance(d.value, bool):
                    is_disjoint = is_disjoint or d.value
        self.min_count = min_count
        self.max_count = max_count
        self.is_disjoint = is_disjoint

    @classmethod
    def constraint_parameters(cls):
        return [SH_qualifiedValueShape, SH_qualifiedMinCount, SH_qualifiedValueShapesDisjoint, SH_qualifiedMaxCount]

    @classmethod
    def constraint_name(cls):
        return "QualifiedValueShapeConstraintComponent"

    def make_generic_messages(self, datagraph: GraphLike, focus_node, value_node) -> List[rdflib.Literal]:
        # TODO:
        #  Implement default message for QualifiedValueConstraint (seems messy)
        return []

    def evaluate(self, target_graph: GraphLike, focus_value_nodes: Dict, _evaluation_path: List):
        """

        :type focus_value_nodes: dict
        :type target_graph: rdflib.Graph
        :type _evaluation_path: List
        """
        reports: List[Dict] = []
        non_conformant = False
        shape = self.shape

        # Shortcut, when there are no value nodes, don't check for recursion, don't validate and exit early
        value_node_count = 0
        for f, value_nodes in focus_value_nodes.items():
            value_node_count = value_node_count + len(value_nodes)
        if value_node_count < 1:
            return (not non_conformant), reports

        potentially_recursive = self.recursion_triggers(_evaluation_path)

        def _evaluate_value_shape(_v_shape):
            nonlocal self, shape, target_graph, focus_value_nodes, _evaluation_path, potentially_recursive
            _reports = []
            _non_conformant = False
            other_shape = shape.get_other_shape(_v_shape)
            if potentially_recursive and other_shape in potentially_recursive:
                warn(ShapeRecursionWarning(_evaluation_path))
                return _non_conformant, _reports
            if not other_shape:
                raise ReportableRuntimeError(
                    "Shape pointed to by sh:property does not exist or is not a well-formed SHACL Shape."
                )
            if self.is_disjoint:
                # Textual Definition of Sibling Shapes:
                # Let Q be a shape in shapes graph G that declares a qualified cardinality constraint (by having values for sh:qualifiedValueShape and at least one of sh:qualifiedMinCount or sh:qualifiedMaxCount). Let ps be the set of shapes in G that have Q as a value of sh:property. If Q has true as a value for sh:qualifiedValueShapesDisjoint then the set of sibling shapes for Q is defined as the set of all values of the SPARQL property path sh:property/sh:qualifiedValueShape for any shape in ps minus the value of sh:qualifiedValueShape of Q itself. The set of sibling shapes is empty otherwise.
                sibling_shapes = set()
                parent_shapes = set(self.shape.sg.subjects(SH_property, self.shape.node))
                for p in iter(parent_shapes):
                    found_siblings = set(self.shape.sg.objects(p, SH_property))
                    for s in iter(found_siblings):
                        if s == self.shape.node:
                            continue
                        sibling_shapes.update(self.shape.sg.objects(s, SH_qualifiedValueShape))

                sibling_shapes = set(self.shape.get_other_shape(s) for s in sibling_shapes)
            else:
                sibling_shapes = set()
            for f, value_nodes in focus_value_nodes.items():
                number_conforms = 0
                for v in value_nodes:
                    try:
                        _is_conform, _r = other_shape.validate(
                            target_graph, focus=v, _evaluation_path=_evaluation_path[:]
                        )
                        if _is_conform:
                            _conforms_to_sibling = False
                            for sibling_shape in sibling_shapes:
                                _c2, _r = sibling_shape.validate(
                                    target_graph, focus=v, _evaluation_path=_evaluation_path[:]
                                )
                                _conforms_to_sibling = _conforms_to_sibling or _c2
                            if not _conforms_to_sibling:
                                number_conforms += 1
                    except ValidationFailure as v:
                        raise v
                if self.max_count is not None and number_conforms > self.max_count:
                    _non_conformant = True
                    _r = self.make_v_result(
                        target_graph, f, constraint_component=SH_QualifiedMaxCountConstraintComponent
                    )
                    _reports.append(_r)
                if self.min_count is not None and number_conforms < self.min_count:
                    _non_conformant = True
                    _r = self.make_v_result(
                        target_graph, f, constraint_component=SH_QualifiedMinCountConstraintComponent
                    )
                    _reports.append(_r)
            return _non_conformant, _reports

        for v_shape in self.value_shapes:
            _nc, _r = _evaluate_value_shape(v_shape)
            non_conformant = non_conformant or _nc
            reports.extend(_r)
        return (not non_conformant), reports


class NodesPathConstraintComponent(ConstraintComponent):
    """
    sh:nodesPath can be used to specify that each value node must conform to the node shape(s) it is connected to via the specified path.
    Link:
    No link yet!
    Textual Definition:
    For each value node v: For each n found at path $path from v that is a node shape, a failure MUST be produced with v as sh:value if the conformance checking of v against n produces a failure. This validation result should include all validation results produced from checking the conformance of v with respect to each n via sh:detail.
    """

    shacl_constraint_component = SH_NodesPathConstraintComponent
    shape_expecting = True
    list_taking = False

    def __init__(self, shape):
        super(NodesPathConstraintComponent, self).__init__(shape)
        nodes_path_objects = set(self.shape.objects(SH_nodesPath))
        if len(nodes_path_objects) < 1:
            raise ConstraintLoadError(
                "NodesPathConstraintComponent must have at least one sh:nodesPath predicate.",
                "No link yet!",
            )
        # Store all values for sh:nodesPath for later
        self.nodes_path_objects = nodes_path_objects
        self.sg = self.shape.sg

    @classmethod
    def constraint_parameters(cls):
        return [SH_nodesPath]

    @classmethod
    def constraint_name(cls):
        return "NodesPathConstraintComponent"

    def make_generic_messages(self, datagraph: GraphLike, focus_node, value_node) -> List[rdflib.Literal]:
        raise NotImplementedError("A Nodes Path Constraint Component should not be able to generate its own message.")

    def evaluate(self, target_graph: GraphLike, focus_value_nodes: Dict, _evaluation_path: List):
        """

        :type focus_value_nodes: dict
        :type target_graph: rdflib.Graph
        :type _evaluation_path: list
        """
        reports: List[Dict] = []
        non_conformant = False
        shape = self.shape

        # Shortcut, when there are no value nodes, don't check for recursion, don't validate and exit early
        value_node_count = 0
        for f, value_nodes in focus_value_nodes.items():
            value_node_count = value_node_count + len(value_nodes)
        if value_node_count < 1:
            return (not non_conformant), reports

        potentially_recursive = self.recursion_triggers(_evaluation_path)

        def _evaluate_nodes_path(path):
            nonlocal shape, target_graph, focus_value_nodes, _evaluation_path, potentially_recursive
            _reports = []
            _non_conformant = False

            for f, value_nodes in focus_value_nodes.items():
                for v in value_nodes:
                    # For each value node, find all nodes at the specifed path from the value node,
                    # discarding the current shape if found in the path to avoid redundancy
                    potential_node_shapes = value_nodes_from_path(self.sg, v, path, target_graph)
                    potential_node_shapes.discard(shape)

                    # Create storage for validation result information on a per focus/value node basis
                    resp_text = resp_node = resp_triples = None

                    for potential_node_shape in potential_node_shapes:
                        # For each node found earlier, proceed validating if it is a node shape
                        # NOTE: shape.get_other_shape returns a Shape object iff that shape has
                        #   already been cached by the SHACL engine, otherwise it returns None.
                        #   In other words, it is basically a dictionary get and no processing
                        #   of the graph is performed. This means that only resources that were
                        #   part of the shapes graph provided will be available for validation here.
                        #   This seems like a reasonable approach/restriction.
                        node_shape = shape.get_other_shape(potential_node_shape)
                        if not node_shape or node_shape.is_property_shape:
                            continue

                        if potentially_recursive and node_shape in potentially_recursive:
                            warn(ShapeRecursionWarning(_evaluation_path))
                            return _non_conformant, _reports

                        # Validate the value node against the shape
                        _is_conform, _r = node_shape.validate(
                            target_graph, focus=v, _evaluation_path=_evaluation_path[:]
                        )
                        if (not _is_conform) or len(_r) > 0:
                            _non_conformant = True
                            # This would be used if the results from the node shape should be added
                            # to the report directly, but that would not capture that this constraint
                            # component was involved at all.
                            # _reports.extend(_r)

                            # Create a validation result for this value node if one hasn't been made already
                            if not resp_node:
                                resp_text, resp_node, resp_triples = self.make_v_result(
                                    datagraph=target_graph,
                                    focus_node=f,
                                    value_node=v,
                                    result_path=path,
                                    extra_messages=[
                                        rdflib.Literal(
                                            f"Value {stringify_node(self.sg.graph, v)} does not conform to shape(s) "
                                            + f"at path {stringify_node(self.sg.graph, path)}. "
                                            + "See details for more information."
                                        )
                                    ],
                                )
                                resp_text += "\tDetails:\n"

                            # Add the report returned for the node shape nested under this new validation result
                            # (via sh:detail in the graph)
                            for text_sub, node_sub, triples_sub in _r:
                                # Add text of validation result in nested details section
                                resp_text = resp_text + indent(text_sub, "\t\t")
                                # Add a triple connecting the new validation result to the
                                # validation result for the nested node
                                resp_triples.append((resp_node, SH_detail, node_sub))
                                # Extend the triples in the report with the ones from the nested result
                                resp_triples.extend(triples_sub)

                    # Append to list of validation results on a focus/value node basis
                    if resp_node:
                        _reports.append((resp_text, resp_node, resp_triples))

            return _non_conformant, _reports

        # Evaluate each object for the predicate sh:nodesPath
        for path in self.nodes_path_objects:
            # Evaluate the node shapes found at that path for all value nodes
            _nc, _r = _evaluate_nodes_path(path)
            non_conformant = non_conformant or _nc
            reports.extend(_r)
        return (not non_conformant), reports
