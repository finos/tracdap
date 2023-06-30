/**
 * A component that maps across each of the business segments and builds a menu that allows the user
 * to browse the categories. This is useful because if you use the {@link SelectBusinessSegment} component
 * is used on its own then you have to know the number of levels and hard code each select separately, where
 * here all this is managed for you. This menu component will show as many levels as specified in the config.
 *
 * @module BusinessSegmentMenu
 * @category Component
 */

import {type AsyncThunk} from "@reduxjs/toolkit";
import {type BusinessSegmentsStoreState, processBusinessSegments} from "./businessSegmentsStore";
import {type ButtonPayload, SearchOption} from "../../../types/types_general";
import Col from "react-bootstrap/Col";
import {Loading} from "../Loading"
import PropTypes from "prop-types";
import React, {useEffect} from "react";
import {type RootState} from "../../../storeController";
import Row from "react-bootstrap/Row";
import {SelectBusinessSegment} from "./SelectBusinessSegment";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the BusinessSegmentsMenu component.
 */
export interface Props {

    /**
     * Whether the business segment inputs should be disabled. This is useful if changing a segment
     * initiates an API call and during this call you need to disable the component so a second call
     * can't be initiated by the user.
     */
    disabled?: boolean
    /**
     * When showing a label this sets whether to show the label in a single row or a stacked column.
     */
    labelPosition?: "left" | "top"
    /**
     * A function that runs when the user changes the selected options. This is a Redux async thunk
     * function. The arguments to the AsyncThunk interface as the type returned by the thunk, the
     * argument to the thunk and the Redux store.
     */
    onChange?: AsyncThunk<SearchOption[], { storeKey: keyof BusinessSegmentsStoreState["uses"] } | ButtonPayload, { state: RootState }>
    /**
     * The key in the BusinessSegmentsStore to get the state for this component.
     */
    storeKey: keyof BusinessSegmentsStoreState["uses"]
}

export const BusinessSegmentMenu = (props: Props) => {

    const {disabled, labelPosition, onChange, storeKey} = props

    // Get what we need from the store
    const {
        levels,
        status: businessSegmentStatus,
    } = useAppSelector(state => state["businessSegmentsStore"].businessSegments)

    const {type} = useAppSelector(state => state["businessSegmentsStore"].uses[storeKey])

    const {
        data,
        fields
    } = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_business_segment_options.currentDefinition)

    const {status: getItemsStatus} = useAppSelector(state => state["applicationSetupStore"].tracItems.getSetupItems)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook that runs whenever the business segment list dataset changes. This dataset is owned by the
     * {@link applicationSetupStore} where the user can create and edit the dataset. If it changes we need to
     * reflect those changes in this component. Furthermore, if the dataset changes we need to remove any of the
     * current values set by the user as there is no guarantee that they are still valid.
     *
     * Note that because we have functions that reset the state of the application (for example if the tenant
     * is changed) we need to add a listener for when to recalculate the processedBusinessSegments object and
     * the available options. We identify this reset by the status property in the store which will also be reset.
     */
    useEffect((): void => {

        // We need to update the options in the UI for the business segments when the API request to
        // get the data from TRAC has completed.
        if (getItemsStatus === "succeeded") {

            dispatch(processBusinessSegments(data))
        }

    }, [data, dispatch, getItemsStatus])

    return (

        <React.Fragment>
            {getItemsStatus === "pending" && <Loading text={"Please wait ..."}/>}

            {getItemsStatus === "succeeded" &&
                <React.Fragment>
                    <Row className={"pb-4"}>
                        {levels.map((level, i) => {

                                // The labels for the selects come from the schema labels for the NAME variables, at index 1,3,5,7
                                // These labels are editable in the application setup scene
                                const label = fields.length >= 2 * (i + 1) ? fields[(2 * i) + 1].label : undefined

                                return (
                                    <Col xs={12} md={6} lg={3} key={level} className={"mt-2 mt-lg-0"}>

                                        <SelectBusinessSegment disabled={disabled}
                                                               isLoading={businessSegmentStatus !== "succeeded"}
                                                               level={level}
                                                               labelPosition={labelPosition}
                                                               labelText={`${label}:` || undefined}
                                                               onChange={onChange}
                                                               showAllAtLoad={Boolean(type === "flat")}
                                                               storeKey={storeKey}
                                        />

                                    </Col>
                                )
                            }
                        )}
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

BusinessSegmentMenu.propTypes = {

    disabled: PropTypes.bool,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    onChange: PropTypes.func,
    storeKey: PropTypes.string.isRequired,
};