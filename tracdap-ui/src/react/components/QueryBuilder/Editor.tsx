/**
 * A component that allows the user to see the query they have built or take control and edit it directly.
 *
 * @module Editor
 * @category Component
 */

import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../ConfirmButton";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import {calculateQueryString, type Query, type QueryBuilderStoreState, resetQuery, setEditorLocking, setUserQuery} from "./queryBuilderStore";
import React, {useMemo} from "react";
import {resetWhereClause, type WhereClause} from "../WhereClauseBuilder/whereClauseBuilderStore";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../SelectValue"
import {ShowHideDetails} from "../ShowHideDetails";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Editor component.
 */
export interface Props {

    /**
     * The object key of the data the user has selected to query, the key include the object version. The
     * store has a separate area for each dataset that the user has loaded up into QueryBuilder.
     */
    objectKey: string
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof QueryBuilderStoreState["uses"]
}

export const Editor = (props: Props) => {

    const {storeKey, objectKey} = props

    console.log(props)
    console.log(useAppSelector(state => state["queryBuilderStore"].uses))

    // Get what we need from the store
    //const {isTextLocked, userQuery} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].editor)
    //const {status} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].execution)

    // Get what we need from the store
    const query: undefined | Query = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey])
    const whereClause: undefined | WhereClause = useAppSelector(state => state["whereClauseBuilderStore"].uses[storeKey].whereClause[objectKey])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Calculate the full query from the user interface UI as a string
    const objectQuery = useMemo(() => query !== null && whereClause != null ? calculateQueryString(query, whereClause) : "", [query, whereClause])

    const isTextLocked = query ? query.editor.isTextLocked : undefined
    const status = query ? query.execution.status : undefined
    const userQuery = query ? query.editor.userQuery : null

    return (

        <ShowHideDetails iconType={"arrow"} linkText={"the SQL editor"} showOnOpen={true}>
            <Card className={"mb-4 pb-3"}>

                <Card.Header>
                    Your SQL query

                    <ConfirmButton ariaLabel={"Reset"}
                                   cancelText={"No"}
                                   className={"ms-2 float-end"}
                                   confirmText={"Yes"}
                                   description={"If you reset the interface all your current selections will be removed, if you want to restore to prior to the reset then the history is available via the undo button, do you want to continue?"}
                                   disabled={status === "pending"}
                                   onClick={() => {
                                       dispatch(resetQuery());
                                       dispatch(resetWhereClause(0))
                                   }}
                                   size={"sm"}
                                   variant={"secondary"}
                    >
                        <Icon ariaLabel={false}
                              className={"me-2"}
                              colour={'#ffffff'}
                              icon={'bi-arrow-counterclockwise'}
                        />Reset
                    </ConfirmButton>

                    <ConfirmButton ariaLabel={isTextLocked ? "Unlock" : "Lock"}
                                   cancelText={"No"}
                                   confirmText={"Yes"}
                                   className={"ms-2 float-end"}
                                   description={isTextLocked ? "If you unlock the query editor you will be able to edit it directly and write more complex queries. You can re-lock the interface but the interface will be reset. Do you want to continue?" : "If you lock the query editor you will lose any changes you make while it was unlocked and the interface will be reset to the position it was in before being edited. Do you want to continue?"}
                                   disabled={status === "pending"}
                                   dispatchedOnClick={setEditorLocking}
                                   size={"sm"}
                                   variant={"secondary"}
                    >
                        <Icon ariaLabel={isTextLocked ? "Locked" : "Unlocked"}
                              className={"me-2"}
                              colour={'#ffffff'}
                              icon={isTextLocked ? 'bi-unlock' : 'bi-lock'}
                        />{isTextLocked ? "Unlock" : "Lock"}
                    </ConfirmButton>

                </Card.Header>

                <Card.Body>

                    <Row>
                        <Col className={"code-lite"}>
                            SELECT
                        </Col>
                    </Row>

                    <SelectValue basicType={trac.STRING}
                                 className={"code-lite mt-3"}
                                 mustValidate={true}
                                 onChange={setUserQuery}
                                 readOnly={isTextLocked}
                                 rows={12}
                                 specialType={"TEXTAREA"}
                                 validationChecked={true}
                                 validateOnMount={false}
                                 value={isTextLocked ? objectQuery : userQuery}
                    />

                </Card.Body>
            </Card>

        </ShowHideDetails>
    )
};

Editor.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};