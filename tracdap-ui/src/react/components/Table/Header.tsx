/**
 * A component that shows a header for a table with the global filter and toolbar icon. It also can show a set of
 * buttons or any component in the header when passed as a prop.
 *
 * @module Header
 * @category Component
 */

import {convertArrayToOptions} from "../../utils/utils_arrays";
import {GlobalFilter} from "./GlobalFilter";
import Nav from 'react-bootstrap/Nav';
import Navbar from 'react-bootstrap/Navbar';
import type {Option, SelectOptionPayload, TableRow} from "../../../types/types_general";
import PropTypes from "prop-types";
import React, {memo} from "react";
import {SelectOption} from "../SelectOption";
import type {Table} from "@tanstack/react-table";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Header component.
 */
export interface Props {

    /**
     * The value of the global filter when the component mounts, if this is updated then the component updates
     * to the new value. This is passed as a prop so that if it changes this memoized component re-renders the new
     * value in the box.
     */
    globalFilterValue: string
    /**
     * A component that is shown in the header along with the toolbar buttons. This is usually a set of buttons that
     * perform tasks such as load something related to the table. This component will be shown on the left of the
     * global search and toolbar icon so that the layout is nice and neat.
     */
    headerComponent?: React.ReactElement<any, any> | React.ReactNode[]
    /**
     * The options for the page length, shown if pagination is being used.
     */
    pageSizeOptions: number[]
    /**
     * Whether to paginate the table.
     */
    paginate: boolean
    /**
     * Whether the global filter widget is being shown.
     */
    showGlobalFilter: boolean
    /**
     * The table object from the react-table plugin.
     */
    table: Table<TableRow>
}

const HeaderInner = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        headerComponent,
        globalFilterValue,
        pageSizeOptions,
        paginate,
        showGlobalFilter,
        table
    } = props

    /**
     * A function that runs when the user changes the page length option. This updates the value in the store.
     * @param payload - The payload from the SelectOption component.
     */
    const handlePageLengthChange = (payload: SelectOptionPayload<Option<number>, false>) => {

        if (payload.value) table.setPageSize(payload.value.value)
    }

    /**
     * A function that runs when the user changes the global filter, the test here is an optimization to prevent
     * unnecessary re-renders due to useHooks inadvertently updating the table state.
     * @param value - The filter value to update in the state.
     */
    const handleGlobalFilterChange = (value: string) => {

        if (table.getState().globalFilter !== value) table.setGlobalFilter(value)
    }

    return (
        <Navbar bg="table" expand={true} className={"ps-2 pe-2 table-navbar"}>
            <Navbar.Collapse>
                <div className="me-auto">
                    {headerComponent}
                </div>
                <Nav>
                    {showGlobalFilter &&
                        <GlobalFilter initialValue={globalFilterValue}
                                      onChange={handleGlobalFilterChange}
                        />
                    }
                    {paginate &&
                        <SelectOption basicType={trac.INTEGER}
                                      className={"me-2 min-width-px-150"}
                                      isDispatched={false}
                                      labelText={"Page length"}
                                      labelPosition={"left"}
                                      onChange={handlePageLengthChange}
                                      options={convertArrayToOptions(pageSizeOptions)}
                                      value={{
                                          value: table.getState().pagination.pageSize,
                                          label: table.getState().pagination.pageSize.toString()
                                      }}
                        />
                    }{children}
                </Nav>
            </Navbar.Collapse>
        </Navbar>
    )
};

HeaderInner.propTypes = {

    children: PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.element]).isRequired,
    headerComponent: PropTypes.element,
    pageSizeOptions: PropTypes.arrayOf(PropTypes.number).isRequired,
    paginate: PropTypes.bool.isRequired,
    showGlobalFilter: PropTypes.bool.isRequired,
    table: PropTypes.object.isRequired
};

export const Header =  memo(HeaderInner);