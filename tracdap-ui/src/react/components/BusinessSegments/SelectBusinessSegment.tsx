/**
 * A component that allows the user to select a business segment, this is used by the {@link BusinessSegmentMenu}
 * component to show a menu of business segments to choose from.
 *
 * @module SelectBusinessSegment
 * @category Component
 */

import type {AsyncThunk} from "@reduxjs/toolkit";
import type {BusinessSegmentsStoreState} from "./businessSegmentsStore";
import {recalculateSegmentOptions} from "./businessSegmentsStore";
import {ButtonPayload, ExtractArrayType, SearchOption} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {memo, useEffect} from "react";
import type {RootState} from "../../../storeController";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {convertKeyToText} from "../../utils/utils_string";

/**
 * An interface for the props of the SelectBusinessSegment component.
 */
export interface Props {

    /**
     * Whether the business segment select should be disabled. This is useful if changing a segment
     * initiates an API call and during this call you need to disable the component so a second call
     * can't be initiated by the user.
     */
    disabled?: boolean
    /**
     * Whether to show a loading icon when an async action is loading in the background.
     */
    isLoading: boolean
    /**
     * When showing a label this sets whether to show the label in a single row or a stacked column.
     */
    labelPosition?: "left" | "top"
    /**
     * The text for the label for the level of options.
     */
    labelText?: string
    /**
     * The level of the options in the hierarchy of groups. The application allows for up to four levels.
     */
    level: ExtractArrayType<BusinessSegmentsStoreState["businessSegments"]["levels"]>
    /**
     * A function that runs when the user changes the selected options. This is a Redux async thunk
     * function. The arguments to the AsyncThunk interface as the type returned by the thunk, the
     * argument to the thunk and the Redux store.
     */
    onChange?: AsyncThunk<SearchOption[], { storeKey: keyof BusinessSegmentsStoreState["uses"] } | ButtonPayload, { state: RootState }>
    /**
     * Whether to show all the SelectBusinessSegment levels after the data hos loaded or to only
     * show them when the user has selected a value for the previous level.
     */
    showAllAtLoad: boolean
    /**
     * The key in the BusinessSegmentsStore to get the state for this component.
     */
    storeKey: keyof BusinessSegmentsStoreState["uses"]
}

const SelectBusinessSegmentInner = (props: Props) => {

    const {
        disabled,
        isLoading,
        labelPosition,
        labelText,
        level,
        onChange,
        showAllAtLoad,
        storeKey,
    } = props

    // Get what we need from the store
    const {
        levels,
        status
    } = useAppSelector(state => state["businessSegmentsStore"].businessSegments)

    const {
        selectedOptions: allSelectedOptions,
        options: allOptions,
        type
    } = useAppSelector(state => state["businessSegmentsStore"].uses[storeKey])

    // Because the key is a variable Typescript requires us to extract the options as separate variables
    let previousLevelSelectedOptions = level === "level_1" ? true : allSelectedOptions[level]
    let selectedOptions = allSelectedOptions[level]
    let options = allOptions[level]

    // Has an option been selected for the business segment prior to this one
    const previousLevelSelected = level === "level_1" ? true : Boolean(previousLevelSelectedOptions)

    // Has the last option been selected
    const lastLevelIsSelected = Boolean(selectedOptions != null)

    // Should we show this business segment selector, in hierarchical mode we only show them when the previous level
    // has had an option picked
    const isReady = Boolean(showAllAtLoad || (status === "succeeded" && previousLevelSelected))

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // If the user sets no business segments then we show a single level 1 option menu with a single 'All' option
    // in this case we override the label as otherwise it uses the default name
    const finalLabelText = levels.length === 1 && options?.length === 1 ? "Business segments:" : labelText || convertKeyToText(level)

    /**
     * A function that runs when the user changes the business segment option. This runs the onChange function
     * to handle what to do now the business segment has been changed e.g. go get me some flows or do some additional calls.
     */
    useEffect(() => {

        // In hierarchical use cases we only want to make a call as a result of changing
        // a business segment when we change the final level segment, or after it has been set.
        // Since there are up to four selects per menu we also want to only make one call per
        // change across the menu (we choose when the last in the menu to make the calls).
        // Remember that all the selects will rerender so all will run this useEffect.
        if (onChange && level === levels[levels.length - 1] && (type === "flat" || lastLevelIsSelected)) {
            dispatch(onChange({storeKey}))
        }

    }, [allSelectedOptions, dispatch, onChange, level, type, lastLevelIsSelected, levels, storeKey])

    return (
        <React.Fragment>
            {isReady && <SelectOption basicType={trac.STRING}
                                      isClearable={Boolean(selectedOptions && selectedOptions.value !== "ALL")}
                                      isDisabled={Boolean(status !== "succeeded" || disabled)}
                                      isDispatched={true}
                                      isLoading={isLoading}
                                      labelPosition={labelPosition}
                                      labelText={finalLabelText}
                                      mustValidate={false}
                                      name={level}
                                      onChange={recalculateSegmentOptions}
                                      options={options}
                                      showValidationMessage={false}
                                      storeKey={storeKey}
                                      value={selectedOptions}
                                      validateOnMount={false}
            />}
        </React.Fragment>
    )
};

SelectBusinessSegmentInner.propTypes = {

    disabled: PropTypes.bool,
    isLoading: PropTypes.bool.isRequired,
    level: PropTypes.string.isRequired,
    labelText: PropTypes.string,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    onChange: PropTypes.func,
    showAllAtLoad: PropTypes.bool.isRequired,
    storeKey: PropTypes.string.isRequired
};

export const SelectBusinessSegment = memo(SelectBusinessSegmentInner);