/**
 * A component that allows the user to add an overlay so that SQL can be defined to run on a dataset.
 *
 * @module AddOverlay
 * @category Component
 */

import {addOverlay} from "./overlayBuilderStore";
import {addWhereClause} from "../WhereClauseBuilder/whereClauseBuilderStore";
import {Button} from "../Button";
import {type ButtonPayload} from "../../../types/types_general";
import Col from "react-bootstrap/Col";
import {Icon} from "../Icon";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppDispatch} from "../../../types/types_hooks";
import PropTypes from "prop-types";

/**
 * An interface for the props of the AddOverlay component.
 */
export interface Props {

    /**
     * Whether the button to add an overlay should be visible.
     */
    showButton: boolean
}

export const AddOverlay = (props: React.PropsWithChildren<Props>) => {

    const {children, showButton} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    return (

        <Row className={"mt-2 mb-5"}>
           <Col>
               {children}
           </Col>
            <Col>
                {showButton &&
                <Button ariaLabel={"Add overlay"}
                        isDispatched={false}
                        onClick={(payload: ButtonPayload) => {
                            dispatch(addOverlay());
                            dispatch(addWhereClause(payload))
                        }}
                        variant={"outline-info"}

                >
                    <Icon ariaLabel={false}
                          className={"me-2"}
                          icon={'bi-plus-lg'}
                    />
                    Add overlay
                </Button>
                }
            </Col>
        </Row>
    )
};

AddOverlay.propTypes = {

    showButton: PropTypes.bool
};