/**
 * A component that allows the user to create an SQL where statement to run on a dataset.
 *
 * @module WhereClauseBuilder
 * @category Component
 */

import {addRule, createWhereClauseEntry, setCurrentlyLoadedWhereClause, type WhereClauseBuilderStoreState} from "./whereClauseBuilderStore";
import {Button} from "../Button";
import Col from "react-bootstrap/Col";
import {Icon} from "../Icon";
import {Loading} from "../Loading";
import PropTypes from "prop-types";
import React, {useEffect} from "react";
import Row from "react-bootstrap/Row";
import {Rules} from "./Rules";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the WhereClauseBuilder component.
 */
export interface Props {

    /**
     * The object key of the data the user has selected to query, the key include the object version. The
     * store has a separate area for each dataset that the user has loaded up into QueryBuilder.
     */
    objectKey: string
    /**
     * The TRAC schema for the dataset.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     *  The key in the WhereClauseBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof WhereClauseBuilderStoreState["uses"]
    /**
     * The index of the where clause, individual objectKey value can have multiple where clauses. For
     * example in OverlayBuilder the objectKey is the dataset key, a dataset can have multiple overlays
     * defined, each with a where clause with multiple rules.
     */
    whereIndex: number
}

export const WhereClauseBuilder = (props: Props) => {

    console.log("Rendering WhereClauseBuilder")

    const {objectKey, schema, storeKey, whereIndex} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook that runs when the page mounts that checks if there is an entry in the store
     * for the selected dataset and if not creates one with all the default settings. This
     * causes an extra re-render on loading the QueryBuilder component but enables us to eliminate
     * a lot of re-renders while the user is using the tool. It also sets the current part of the
     * store in use.
     */
    useEffect(() => {

        // Update the where clause builder store
        dispatch(setCurrentlyLoadedWhereClause({storeKey, objectKey}))
        dispatch(createWhereClauseEntry({storeKey, objectKey, schema}))

    }, [dispatch, storeKey, objectKey, schema])

    // Get what we need from the store
    const {objectKey: currentlyLoadedObjectKey} = useAppSelector(state => state["whereClauseBuilderStore"].currentlyLoaded)

    // If in entry for the dataset does not yet exist don't show the interface. The useEffect hook will create one.
    if (currentlyLoadedObjectKey == null || currentlyLoadedObjectKey !== objectKey) {
        return (<Loading/>)
    }

    return (

        <React.Fragment>

            <Row>
                <Col xs={12}>
                    <Button ariaLabel={"Add rule"}
                            isDispatched={true}
                            index={whereIndex}
                            onClick={addRule}
                            variant={"outline-info"}

                    >
                        <Icon ariaLabel={false}
                              className={"me-2"}
                              icon={'bi-plus-lg'}
                        />
                        Add rule
                    </Button>
                </Col>
            </Row>

            <Rules {...props}/>

        </React.Fragment>
    )
};

WhereClauseBuilder.propTypes = {

    objectKey: PropTypes.string.isRequired,
    schema: PropTypes.array.isRequired,
    storeKey: PropTypes.string.isRequired
};