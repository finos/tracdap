/**
 * A component that shows a single set of logic options that join two rules together in the where clause.
 *
 * @module Logic
 * @category Component
 */

import {Button} from "../Button";
import Col from "react-bootstrap/Col";
import PropTypes from "prop-types";
import React, {memo} from "react";
import Row from "react-bootstrap/Row";
import {type Rule, setLogic} from "./whereClauseBuilderStore";

/**
 * An interface for the props of the Logic component.
 */
export interface Props {

    /**
     * The 'AND' or 'OR' logic o apply
     */
    logic: Rule["logic"]
    /**
     * Whether the not logic is on.
     */
    not: Rule["not"]
    /**
     *  The index of the rule, this is sent back when updating the rule, so we know which index to update.
     */
    ruleIndex: number
    /**
     * The index of the where clause, individual objectKey value can have multiple where clauses. For
     * example in OverlayBuilder the objectKey is the dataset key, a dataset can have multiple overlays
     * defined, each with a where clause with multiple rules.
     */
    whereIndex: number
}

const LogicInner = (props: Props) => {

    const {logic, not, ruleIndex, whereIndex} = props

    return (

        <Row>
            <Col className={"mt-3 text-center"}>
                <Button ariaLabel={"and"}
                        className={"me-1"}
                        id={ruleIndex}
                        index={whereIndex}
                        isDispatched={true}
                        name={"AND"}
                        onClick={setLogic}
                        size={"sm"}
                        variant={logic === "AND" ? "secondary" : "outline-secondary"}
                >
                    AND
                </Button>
                <Button ariaLabel={"or"}
                        className={"me-1"}
                        id={ruleIndex}
                        index={whereIndex}
                        isDispatched={true}
                        name={"OR"}
                        onClick={setLogic}
                        size={"sm"}
                        variant={logic === "OR" ? "secondary" : "outline-secondary"}
                >
                    OR
                </Button>
                <Button ariaLabel={"not"}
                        className={"me-0"}
                        id={ruleIndex}
                        index={whereIndex}
                        onClick={setLogic}
                        size={"sm"}
                        variant={not ? "secondary" : "outline-secondary"}
                >
                    NOT
                </Button>
            </Col>
        </Row>
    )
};

LogicInner.propTypes = {

    logic: PropTypes.oneOf(["AND", "OR"]).isRequired,
    not: PropTypes.bool.isRequired,
    ruleIndex: PropTypes.number.isRequired,
    whereIndex: PropTypes.number.isRequired
};

export const Logic = memo(LogicInner);