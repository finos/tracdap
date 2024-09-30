import {Burger} from "../../../components/Burger";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import {Icon} from "../../../components/Icon";
import {ObjectSummaryStoreState, setFlowDirection, setSelectedTool, setShowRenames} from "../store/objectSummaryStore";
import PropTypes from "prop-types";
import React, {memo, useState} from "react";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * A component that shows a toolbar to allow the user to navigate an SVG.
 */

type Props = {

    /**
     * Whether the SVG can be exported.
     */
    canExport: boolean
    /**
     * A function to run that downloads the SVG in the DOM as a file.
     */
    downloadSvg: () => void
    /**
     * The function to run to reset the SVG viewer to its initial view.
     */
    fitToViewer: () => void
    /**
     * Whether the flow contains datasets that are renamed between being the output of a
     * model and being used as the input of another model.
     */
    hasRenames: boolean
    /**
     * The direction of the buttons in the toolbar. This is used depending on the
     * size of the user's scree the toolbar is placed in a more discrete place that
     * takes up less of the screen width.
     */
    toolbarDirection: "vertical" | "horizontal"
    /**
     * The key in the objectSummaryStore to get the state for this component.
     */
    storeKey: keyof ObjectSummaryStoreState["flow"]
};

const SvgToolbar = (props: Props) => {

    const {
        canExport,
        downloadSvg,
        fitToViewer,
        hasRenames,
        storeKey,
        toolbarDirection
    } = props

    // Whether the submenu is open or not
    const [burgerOpen, setBurgerOpen] = useState<boolean>(false)

    // Get what we need from the store
    const {
        flowDirection,
        showRenames,
        selectedTool
    } = useAppSelector(state => state["objectSummaryStore"].flow[storeKey].chart)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    return (
        <div className={`d-flex ${toolbarDirection === "horizontal" ? "flex-row-reverse" : "flex-column"}`}>
            <Dropdown
                className={`d-flex ${toolbarDirection === "vertical" ? "justify-content-center" : "justify-content-end"}`}
                drop={toolbarDirection === "horizontal" ? "up" : "start"}
                onToggle={isOpen => setBurgerOpen(isOpen)}
            >
                <Dropdown.Toggle bsPrefix={"p-0"}
                                 className={`no-halo ${toolbarDirection === "horizontal" ? "ps-2" : "pb-2"} ${canExport ? "visible" : "invisible"}`}
                                 id="chart-menu-button"
                                 size={"sm"}
                                 title="Dropdown button"
                                 variant={"link"}
                >
                    <Burger ariaLabel={"Menu"} open={burgerOpen} size={"sm"}/>
                </Dropdown.Toggle>

                <Dropdown.Menu className={`${toolbarDirection === "horizontal" ? " pe-2" : "pt-2"}`}>

                    <Dropdown.Item className={"fs-8 px-3"}
                                   onClick={downloadSvg}
                                   size={"sm"}
                    >
                        Download image
                    </Dropdown.Item>
                    <Dropdown.Item className={"fs-8 px-3"}
                                   onClick={() => dispatch(setFlowDirection({
                                       storeKey,
                                       flowDirection: flowDirection === "RIGHT" ? "DOWN" : "RIGHT"
                                   }))}
                                   size={"sm"}
                    >
                        {flowDirection === "RIGHT" ? "Draw top to bottom" : "Draw left to right"}
                    </Dropdown.Item>
                    {hasRenames &&

                        <Dropdown.Item className={"fs-8 px-3"}
                                       onClick={() => dispatch(setShowRenames({storeKey, showRenames: !showRenames}))}
                                       size={"sm"}
                        >
                            {showRenames === true ? "Hide renamed datasets" : "Show renamed datasets"}
                        </Dropdown.Item>
                    }

                </Dropdown.Menu>
            </Dropdown>

            <ButtonGroup aria-label="SVG image viewer controls"
                         className={toolbarDirection === "horizontal" ? undefined : "mx-auto"}
                         size={"sm"}
                         vertical={Boolean(toolbarDirection === "vertical")}
            >
                <Button active={Boolean(selectedTool.real === "none")}
                        className={`no-halo ${toolbarDirection === "horizontal" ? "min-width-px-60" : undefined}`}
                        onClick={e => {
                            // Make sure that the button and the icon onClicks don't both run
                            e.stopPropagation();
                            dispatch(setSelectedTool({storeKey, selectedTool: {real: "none", override: "none"}}))
                        }}
                        variant="dark"
                >
                    <Icon ariaLabel={false}
                           colour={selectedTool.real === "none" ? "inherit" : "var(--info)"}
                           icon={"bi-hand-index-thumb"}
                           size={"1.5rem"}
                    />
                </Button>
                <Button
                    active={Boolean(selectedTool.real === "pan")}
                    className={`no-halo ${toolbarDirection === "horizontal" ? "min-width-px-60" : undefined}`}
                    onClick={e => {
                        // Make sure that the button and the icon onClicks don't both run
                        e.stopPropagation();
                        dispatch(setSelectedTool({storeKey, selectedTool: {real: "pan", override: "pan"}}))
                    }}
                    variant="dark"
                >
                    <Icon ariaLabel={false}
                           colour={selectedTool.real === "pan" ? "inherit" : "var(--info)"}
                           icon={"bi-arrows-move"}
                           size={"1.5rem"}
                    />
                </Button>
                <Button active={Boolean(selectedTool.real === "zoom-in")}
                        className={`no-halo ${toolbarDirection === "horizontal" ? "min-width-px-60" : undefined}`}
                        onClick={e => {
                            // Make sure that the button and the icon onClicks don't both run
                            e.stopPropagation();
                            // We don't want to use the default zoom in function of the SVG plugin
                            // instead we want to record that that's what the user wanted to do
                            // but instead use the bespoke functions built specifically. This is
                            // to make the colours of the marquee what we want and also to better
                            // get the zoomed in area centered.
                            dispatch(setSelectedTool({storeKey, selectedTool: {real: "zoom-in", override: "none"}}))
                        }}
                        variant="dark"
                >
                    <Icon ariaLabel={false}
                           colour={selectedTool.real === "zoom-in" ? "inherit" : "var(--info)"}
                           icon={"bi-plus-square-dotted"}
                           size={"1.5rem"}
                    />
                </Button>
                <Button active={Boolean(selectedTool.real === "zoom-out")}
                        className={`no-halo ${toolbarDirection === "horizontal" ? "min-width-px-60" : undefined}`}
                        onClick={e => {
                            // Make sure that the button and the icon onClicks don't both run
                            e.stopPropagation();
                            dispatch(setSelectedTool({
                                storeKey,
                                selectedTool: {real: "zoom-out", override: "zoom-out"}
                            }))
                        }}
                        variant="dark"
                >
                    <Icon ariaLabel={false}
                           colour={selectedTool.real === "zoom-out" ? "inherit" : "var(--info)"}
                           icon={"bi-zoom-out"}
                           size={"1.5rem"}
                    />
                </Button>
                <Button className={"no-halo"}

                        onClick={e => {
                            // Make sure that the button and the icon onClicks don't both run
                            e.stopPropagation();
                            return fitToViewer()
                        }}
                        variant="dark"
                >
                    <Icon ariaLabel={false}
                           icon={"bi-arrows-fullscreen"}
                           size={"1.5rem"}
                           colour={"var(--info)"}
                    />
                </Button>
            </ButtonGroup>
        </div>
    )
};

SvgToolbar.propTypes = {

    canExport: PropTypes.bool,
    downloadSvg: PropTypes.func.isRequired,
    fitToViewer: PropTypes.func.isRequired,
    hasRenames: PropTypes.bool.isRequired,
    storeKey: PropTypes.string.isRequired,
    toolbarDirection: PropTypes.oneOf(["vertical", "horizontal"]).isRequired
};

SvgToolbar.defaultProps = {

    canExport: true
};

export default memo(SvgToolbar);