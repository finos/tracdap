import {convertDateObjectToIsoString} from "../utils/utils_general";
import endOfDay from "date-fns/endOfDay";
import endOfMonth from "date-fns/endOfMonth";
import Form from "react-bootstrap/Form";
import {isDateFormat} from "../utils/utils_trac_type_chckers";
import {LabelLayout} from "./LabelLayout";
import parseISO from "date-fns/parseISO";
import PropTypes from "prop-types"
import React, {memo} from "react";
import {SelectDate} from "./SelectDate";
import {SelectDateRangeProps, DateFormat, DatetimeFormat} from "../../types/types_general";
import startOfDay from "date-fns/startOfDay";
import startOfMonth from "date-fns/startOfMonth";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../types/types_hooks";

/**
 This component allows the user to select a date range. It is based on the
 SelectDate component which uses the react-datepicker package.
 */

const SelectDateRange = (props: SelectDateRangeProps) => {

    const {
        className,
        createOrUpdate,
        dayOrMonth,
        endDate,
        id,
        isDisabled,
        isDispatched,
        name,
        onChange,
        setDayOrMonth,
        setCreateOrUpdate,
        startDate,
        storeKey
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that ensures that the start date is at the right time to encompass the entire range implied.
     */
    const actualStartDate = (dayOrMonth: DateFormat | DatetimeFormat) => {

        if (dayOrMonth === "DAY") {
            return convertDateObjectToIsoString(startOfDay(parseISO(startDate)), "datetimeIso")

        } else {
            return convertDateObjectToIsoString(startOfMonth(parseISO(startDate)), "datetimeIso")
        }
    }

    /**
     * A function that ensures that the end date is at the right time to encompass the entire range implied.
     */
    const actualEndDate = (dayOrMonth: DateFormat | DatetimeFormat) => {

        if (dayOrMonth === "DAY") {
            return convertDateObjectToIsoString(endOfDay(parseISO(endDate)), "datetimeIso")

        } else {
            return convertDateObjectToIsoString(endOfMonth(parseISO(endDate)), "datetimeIso")
        }
    }

    /**
     * A function that runs when the user changes to search based on day or month. It is run by the radio button onChange
     * event. The start date and end date are also updated as they need to match with the selected date format.
     * @param e - The event that triggered the function.
     */
    const handleDayOrMonthChange = (e: React.ChangeEvent<HTMLInputElement>): void => {

        if (isDateFormat(e.target.id)) {

            const newStartDate = actualStartDate(e.target.id)
            const newEndDate = actualEndDate(e.target.id)
            dispatch(setDayOrMonth({storeKey, dayOrMonth: e.target.id, startDate: newStartDate, endDate: newEndDate}))
        }
    }

    /**
     * A function that runs when the user changes to search based on creation or update date. It is run by the radio
     * button onChange event.
     * @param e - The event that triggered the function.
     */
    const handleUpdateOrCreateChange = (e: React.ChangeEvent<HTMLInputElement>): void => {

        dispatch(setCreateOrUpdate({storeKey, createOrUpdate: e.target.id, id, name}))
    }

    return (
        <React.Fragment>
            <LabelLayout {...props}>
                <div className={"d-flex align-items-center"}>

                    <Form.Check checked={createOrUpdate === "CREATE"}
                                className={"me-3"}
                                id={"CREATE"}
                                inline
                                label="Created"
                                onChange={handleUpdateOrCreateChange}
                                type={"radio"}
                    />

                    <Form.Check checked={createOrUpdate === "UPDATE"}
                                id={"UPDATE"}
                                inline
                                label={"Updated"}
                                onChange={handleUpdateOrCreateChange}
                                type={"radio"}
                    />

                    <SelectDate basicType={trac.DATETIME}
                                className={className}
                                formatCode={dayOrMonth}
                                id={id}
                                isClearable={false}
                                isDisabled={isDisabled}
                                isDispatched={isDispatched}
                                maximumValue={endDate}
                                minimumValue={undefined}
                                mustValidate={false}
                                name={"startDate"}
                                onChange={onChange}
                                position={"start"}
                                showValidationMessage={false}
                                storeKey={storeKey}
                                validationChecked={false}
                                validateOnMount={false}
                                value={startDate}
                    />

                    <span className={"mx-3"}>to</span>

                    <SelectDate basicType={trac.DATETIME}
                                className={className}
                                formatCode={dayOrMonth}
                                id={id}
                                isClearable={false}
                                isDisabled={isDisabled}
                                isDispatched={isDispatched}
                                maximumValue={undefined}
                                minimumValue={startDate}
                                mustValidate={false}
                                name={"endDate"}
                                onChange={onChange}
                                position={"end"}
                                showValidationMessage={false}
                                storeKey={storeKey}
                                validationChecked={false}
                                validateOnMount={false}
                                value={endDate}
                    />

                    <Form.Check checked={dayOrMonth === "DAY"}
                                className={"ms-3"}
                                id={"DAY"}
                                inline
                                label={"Day"}
                                onChange={handleDayOrMonthChange}
                                type={"radio"}
                    />

                    <Form.Check checked={dayOrMonth === "MONTH"}
                                id={"MONTH"}
                                inline
                                label="Month"
                                onChange={handleDayOrMonthChange}
                                type={"radio"}
                    />
                </div>
            </LabelLayout>
        </React.Fragment>
    )
};

SelectDateRange.propTypes = {

    className: PropTypes.string.isRequired,
    id: PropTypes.string,
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    onChange: PropTypes.func.isRequired,
    startDate: PropTypes.string.isRequired,
    tooltip: PropTypes.string,
    endDate: PropTypes.string.isRequired,
};

SelectDateRange.defaultProps = {

    className: "",
    isDisabled: false,
    isDispatched: true,
    labelPosition: "top"
};

export default memo(SelectDateRange);