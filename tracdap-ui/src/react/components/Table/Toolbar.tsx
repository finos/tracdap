/**
 * A component that shows a burger icon that when clicked on shows a toolbar of table options to the user.
 *
 * @module Toolbar
 * @category Component
 */

import {Burger} from "../Burger";
import {DataInfoModal} from "../DataInfoModal";
import Dropdown from "react-bootstrap/Dropdown";
import {General} from "../../../config/config_general";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import React, {memo, useCallback, useState} from "react";
import type {SortingState, Table, VisibilityState} from "@tanstack/react-table";
import type {ColumnColourDefinitions, FileImportModalPayload, TableRow, TracUiTableState} from "../../../types/types_general";
import {ColumnColoursModal} from "./ColumnColoursModal";
import {ColumnVisibilityModal} from "./ColumnVisibilityModal";
import {FileImportModal} from "../FileImportModal/FileImportModal";
import {useAppDispatch} from "../../../types/types_hooks";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the Toolbar component.
 */
export interface Props {

    /**
     * A set of definitions for the heatmaps and traffic lights to apply to the table, the user can open a modal
     * and edit these.
     */
    columnColours: ColumnColourDefinitions
    /**
     * Whether columns are visible in the table, this can be retrieved from the table prop using
     * table.getState().columnVisibility, but we need changes to this to trigger a rerender, so we pass it as a prop.
     */
    columnVisibility: VisibilityState
    /**
     * The table data, used to set the heatmap and traffic light definitions if the user adds a new one.
     */
    data: TableRow[]
    /**
     * Whether the buttons in the toolbar should be disabled, for example if the data is being downloaded.
     */
    disabled: boolean
    /**
     * The function to run when exporting the data as a csv, this is passed as a prop as other information such as
     * the data, metadata and the username need to be passed as part of the export, and we don't want to prop drill.
     */
    handleCsvExport: () => void
    /**
     * The function to run when exporting the data as an Excel file, this is passed as a prop as other information such
     * as the data, metadata and the username need to be passed as part of the export, and we don't want to prop drill.
     */
    handleXlsxExport: () => void
    /**
     * A function that updates the data for the table, this is assumed to be in a Redux store so the function needs to
     * be dispatched. This is used when uploading data and editing data. When this is set an "Import from file" option
     * will appear in the toolbar.
     */
    importDataFunction?: Function
    /**
     * The initial sort applied to the table on mounting. If there is an initial sort set on the table
     * when it mounts we show a button that allows the sorting to be reset to this sorting.
     */
    initialSort: SortingState | undefined
    /**
     * Whether the data is editable by the user.
     */
    isEditable: boolean
    /**
     * Whether to restrict some functionality due to the table being large.
     */
    restrictedFunctionality: boolean
    /**
     * The schema for the dataset.
     */
    schema: trac.metadata.IFieldSchema[]
    /**
     * The function to run when we need to update the table state in the parent. For example toggling the column filter
     * visibility, the column order widgets, whether to show the global filter widget and update the heatmap or
     * traffic light definitions.
     */
    setTracUiState: React.Dispatch<React.SetStateAction<TracUiTableState>>
    /**
     * Whether the column filters widget row is being shown.
     */
    showColumnFilters: boolean
    /**
     * Whether the column order widget row is being shown.
     */
    showColumnOrder: boolean
    /**
     * Whether to allow the data to be exported to csv or excel.
     */
    showExportButtons: boolean
    /**
     * Whether the global filter widget is being shown.
     */
    showGlobalFilter: boolean
    /**
     * The key in a store to use to save the data to/get the data from.
     */
    storeKey?: string
    /**
     * The table object from the react-table plugin.
     */
    table: Table<TableRow>
    /**
     * The TRAC metadata for the data.
     */
    tag?: trac.metadata.ITag
}

const ToolbarInner = (props: Props) => {

    const {
        data,
        disabled,
        handleCsvExport,
        handleXlsxExport,
        initialSort,
        showColumnFilters,
        showColumnOrder,
        showExportButtons,
        showGlobalFilter,
        table,
        columnColours,
        setTracUiState,
        importDataFunction,
        storeKey,
        isEditable,
        restrictedFunctionality,
        schema,
        tag
    } = props

    // Whether the submenu is open or not
    const [burgerOpen, setBurgerOpen] = useState(false)

    // Control the modals
    const [showHeatmapModal, setShowHeatmapModal] = React.useState<boolean>(false)
    const [showColumnVisibilityModal, setShowColumnVisibilityModal] = React.useState<boolean>(false)
    const [showImportModal, setShowImportModal] = React.useState<boolean>(false)
    const [showInfoModal, setShowInfoModal] = React.useState<boolean>(false)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user opens or closes the upload modal.
     */
    const onToggleImportModal = useCallback(() => {

        setShowImportModal(!showImportModal)

    }, [showImportModal])

    return (
        <React.Fragment>
            <Dropdown className={"my-auto"}
                      drop={"start"}
                      onToggle={isOpen => setBurgerOpen(isOpen)}
            >
                <Dropdown.Toggle id="object-menu-button"
                                 title="Dropdown button"
                                 size={"sm"}
                                 variant={"link"}
                                 className={`no-halo`}
                                 bsPrefix={"p-0"}
                >
                    <Burger ariaLabel={"Menu icon"} size={"md"} open={burgerOpen}/>
                </Dropdown.Toggle>

                <Dropdown.Menu className={"py-2"}>

                    {tag &&
                        <Dropdown.Item size={"sm"}
                                       onClick={() => setShowInfoModal(true)}
                                       className={"fs-8 px-3"}
                                       disabled={disabled}
                        >
                            <Icon ariaLabel={false}
                                  className={"pe-2"}
                                  icon={"bi-info-circle"}
                            />
                            <span>Info</span>
                        </Dropdown.Item>
                    }

                    <Dropdown.Item size={"sm"}
                                   onClick={() => setShowColumnVisibilityModal(true)}
                                   className={"fs-8 px-3"}
                                   disabled={disabled}
                    >
                        <Icon ariaLabel={false}
                              className={"pe-2"}
                              icon={"bi-eye"}
                        />
                        <span>Change column visibility</span>
                    </Dropdown.Item>

                    <Dropdown.Item size={"sm"}
                                   onClick={() => setShowHeatmapModal(true)}
                                   className={"fs-8 px-3"}
                                   disabled={disabled}
                    >
                        <Icon ariaLabel={false}
                              className={"pe-2"}
                              icon={"bi-map"}
                        />
                        <span>Heatmap & traffic lights</span>
                    </Dropdown.Item>

                    <Dropdown.Item size={"sm"}
                                   onClick={() => setTracUiState(prevState => ({...prevState, ...{showColumnOrder: !prevState.showColumnOrder}}))}
                                   className={"fs-8 px-3"}
                                   disabled={disabled}
                    >
                        <Icon ariaLabel={false}
                              className={"pe-2"}
                              icon={"bi-arrow-left-right"}
                        />
                        <span>{showColumnOrder ? "Hide" : "Change"} column ordering</span>
                    </Dropdown.Item>

                    {!restrictedFunctionality &&
                        <React.Fragment>
                            <Dropdown.Item size={"sm"}
                                           onClick={() => setTracUiState(prevState => ({...prevState, ...{showGlobalFilter: !prevState.showGlobalFilter}}))}
                                           className={"fs-8 px-3"}
                                           disabled={disabled}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-search"}
                                />
                                <span>{showGlobalFilter ? "Hide" : "Show"} table search</span>
                            </Dropdown.Item>

                            <Dropdown.Item size={"sm"}
                                           onClick={() => setTracUiState(prevState => ({...prevState, ...{showColumnFilters: !prevState.showColumnFilters}}))}
                                           className={"fs-8 px-3"}
                                           disabled={disabled}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-layout-sidebar-inset-reverse"}
                                />
                                <span>{showColumnFilters ? "Hide" : "Show"} column search</span>
                            </Dropdown.Item>
                        </React.Fragment>
                    }

                    {/*Only show the reset sorting option if the table mounts with a sorting applied*/}
                    {!restrictedFunctionality && table &&
                        <React.Fragment>

                            <div className="dropdown-divider"/>

                            {(initialSort && initialSort.length !== 0) &&
                                <Dropdown.Item size={"sm"}
                                               onClick={() => table.resetSorting()}
                                               className={"fs-8 px-3"}
                                               disabled={disabled}
                                >
                                    <Icon ariaLabel={false}
                                          className={"pe-2"}
                                          icon={"bi-list"}
                                    />
                                    <span>Reset sorting</span>
                                </Dropdown.Item>
                            }

                            <Dropdown.Item size={"sm"}
                                           onClick={() => table.resetSorting(true)}
                                           className={"fs-8 px-3"}
                                           disabled={disabled}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-sort-alpha-down"}
                                />
                                <span>Remove all sorting</span>
                            </Dropdown.Item>


                            <div className="dropdown-divider"/>

                            <Dropdown.Item size={"sm"}
                                           onClick={() => {
                                               table.setGlobalFilter("");
                                               table.resetColumnFilters(true)
                                           }}
                                           className={"fs-8 px-3"}
                                           disabled={disabled}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-list"}
                                />
                                <span>Remove all filtering</span>
                            </Dropdown.Item>
                        </React.Fragment>
                    }

                    {showExportButtons && General.tables.exportFileTypes.length > 0 &&
                        <div className="dropdown-divider"/>
                    }

                    {showExportButtons && General.tables.exportFileTypes.includes("csv") &&
                        <Dropdown.Item size={"sm"}
                                       onClick={handleCsvExport}
                                       className={"fs-8 px-3"}
                                       disabled={disabled}
                        >
                            <Icon ariaLabel={false}
                                  className={"pe-2"}
                                  icon={"bi-filetype-csv"}
                            />
                            <span>Export as csv</span>
                        </Dropdown.Item>
                    }
                    {showExportButtons && General.tables.exportFileTypes.includes("xlsx") &&
                        <Dropdown.Item size={"sm"}
                                       onClick={handleXlsxExport}
                                       className={"fs-8 px-3"}
                                       disabled={disabled}
                        >
                            <Icon ariaLabel={false}
                                  className={"pe-2"}
                                  icon={"bi-filetype-xlsx"}
                            />
                            <span>Export as xslx</span>
                        </Dropdown.Item>
                    }

                    {isEditable && importDataFunction && General.tables.importFileTypes.length > 0 &&
                        <React.Fragment>
                            <div className="dropdown-divider"/>
                            <Dropdown.Item size={"sm"}
                                           onClick={onToggleImportModal}
                                           className={"fs-8 px-3"}
                                           disabled={disabled}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-file-earmark-arrow-up"}
                                />
                                <span>Import from file</span>
                            </Dropdown.Item>
                        </React.Fragment>
                    }

                </Dropdown.Menu>
            </Dropdown>
            {/*Mount the modal each time it is shown, note that we tuck the modals here rather than in the table
            component because although there is a lot of prop drilling it means that when you show or hide a modal
            the state of the table component does not change - that happens here - so the table is not redrawn*/}
            {showHeatmapModal &&
                <ColumnColoursModal data={data}
                                    show={showHeatmapModal}
                                    table={table}
                                    toggle={setShowHeatmapModal}
                                    columnColours={columnColours}
                                    setTracUiState={setTracUiState}
                />
            }
            {showColumnVisibilityModal &&
                <ColumnVisibilityModal show={showColumnVisibilityModal}
                                       table={table}
                                       toggle={setShowColumnVisibilityModal}
                />
            }
            {importDataFunction && isEditable &&
                <FileImportModal show={showImportModal}
                                 onToggleModal={onToggleImportModal}
                                 allowedFileTypes={General.tables.importFileTypes}
                                 allowedSchema={schema}
                                 returnImportedData={(payload: FileImportModalPayload) => {
                                     if (Array.isArray(payload.data)) dispatch(importDataFunction({
                                         storeKey: storeKey,
                                         data: payload.data
                                     }))
                                 }}
                />
            }
            {tag && showInfoModal &&
                <DataInfoModal show={showInfoModal} toggle={() => setShowInfoModal(!showInfoModal)} tagSelector={tag.header}/>
            }
        </React.Fragment>
    )
};

ToolbarInner.propTypes = {

    columnVisibility: PropTypes.object.isRequired,
    data: PropTypes.array.isRequired,
    disabled: PropTypes.bool.isRequired,
    handleCsvExport: PropTypes.func.isRequired,
    handleXlsxExport: PropTypes.func.isRequired,
    initialSort: PropTypes.arrayOf(PropTypes.object),
    onShowInfoModal: PropTypes.func,
    setTracUiState: PropTypes.func.isRequired,
    showColumnFilters: PropTypes.bool.isRequired,
    showColumnOrder: PropTypes.bool.isRequired,
    showExportButtons: PropTypes.bool.isRequired,
    showGlobalFilter: PropTypes.bool.isRequired,
    table: PropTypes.object.isRequired
};

export const Toolbar = memo(ToolbarInner);