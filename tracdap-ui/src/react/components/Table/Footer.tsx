/**
 * A component that shows a footer for a table with information and pagination details. It also can show a set of
 * buttons or any component in the footer when passed as a prop.
 *
 * @module Footer
 * @category Component
 */

import {Information} from "./Information";
import {Pagination} from "./Pagination";
import PropTypes from "prop-types";
import React from "react";
import {Table} from "@tanstack/react-table";
import {TableRow} from "../../../types/types_general";

/**
 * An interface for the props of the Footer component.
 */
export interface Props {

    /**
     * A component that is shown in the footer along with the pagination buttons. This is usually a set of buttons that
     * perform tasks such as load something related to the table. This component will be shown next to the pagination
     * buttons and the table information so that the layout is nice and neat.
     */
    footerComponent?: React.ReactElement<any, any> | React.ReactNode[]
    /**
     * Whether to paginate the table.
     */
    paginate: boolean
    /**
     * Whether to show information about the table data.
     */
    showInformation: boolean
    /**
     * The table object from the react-table plugin.
     */
    table: Table<TableRow>
}

export const Footer = (props: Props) => {

    const {
        footerComponent,
        paginate,
        showInformation,
        table
    } = props

    return (

        (paginate || showInformation || footerComponent) ?
            <div className={`d-flex ${footerComponent != undefined ? "mt-2 mt-md-4" : "mt-2"} flex-column flex-md-row align-items-center justify-content-md-between`}>
                {showInformation &&
                    <div className={"mt-0 mb-2 mb-md-0 fs-13 me-0 me-md-2"}>
                        <Information filteredLength={table.getFilteredRowModel().rows.length}
                                     pageCount={table.getPageCount()}
                                     pageIndex={table.getState().pagination.pageIndex}
                                     pageSize={table.getState().pagination.pageSize}
                                     paginate={paginate}
                                     rowsLength={table.getCoreRowModel().rows.length}
                                     selectedLength={table.getSelectedRowModel().rows.length}
                                     selectedAndFilteredLength={table.getFilteredSelectedRowModel().rows.length}
                        />
                    </div>
                }

                <div>
                    {footerComponent}
                    {paginate &&
                        <Pagination canNextPage={table.getCanNextPage()}
                                    canPreviousPage={table.getCanPreviousPage()}
                                    className={footerComponent ? "pe-2" : ""}
                                    nextPage={table.nextPage}
                                    pageCount={table.getPageCount()}
                                    previousPage={table.previousPage}
                                    setPageIndex={table.setPageIndex}
                        />
                    }
                </div>

            </div> : null
    )
};

Footer.propTypes = {

    footerComponent: PropTypes.element,
    paginate: PropTypes.bool.isRequired,
    table: PropTypes.object.isRequired,
    showInformation: PropTypes.bool.isRequired
};