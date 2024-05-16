/**
 * A component that shows a drag and drop box that the user can drag a file onto and trigger
 * an action. See {@link https://medium.com/@650egor/simple-drag-and-drop-file-upload-in-react-2cb409d88929| this guide}.
 *
 * @module DragAndDrop
 * @category Component
 */

import PropTypes from "prop-types";
import React, {useCallback, useEffect, useRef, useState} from "react";

/**
 * An interface for the props of the DragAndDrop component.
 */
export interface Props {

    /**
     * The css class to apply to the box, this allows additional styles to be added to the component.
     */
    className?: string
    /**
     * A function that runs when the user drops a dragged file into the drag area. This receives the list of files.
     */
    onDrop: (files: FileList) => void
}

export const DragAndDrop = (props: React.PropsWithChildren<Props>) => {

    const {
        children,
        className = "",
        onDrop
    } = props

    // This is a reference to the drag area in the DOM
    const dropArea = useRef<HTMLDivElement>(null)

    // This is a counter used to work out if the user has entered the drag area
    const dragCounter = useRef<number>(0)

    const [dragging, setDragging] = useState(false)

    /**
     * A function that runs when the user drags a file into the drag area.
     * @param event - The drag event that triggered the function.
     */
    const handleDragIn = (event: DragEvent): void => {

        // Prevent any events in the DOM from being triggered
        event.preventDefault()
        event.stopPropagation()

        // Add one to the drag counter - it tells us whether we are in or out but does not cause a rerender
        dragCounter.current++

        if (event.dataTransfer && event.dataTransfer.items.length > 0) {
            setDragging(true)
        }
    }

    /**
     * A function that runs when the user drags a file out of the drag area.
     * @param event - The drag event that triggered the function.
     */
    const handleDragOut = (event: DragEvent): void => {

        // Prevent any events in the DOM from being triggered
        event.preventDefault()
        event.stopPropagation()

        // Take one off the drag counter - it tells us whether we are in or out but does not cause a rerender
        dragCounter.current--

        if (dragCounter.current > 0) return
        setDragging(false)
    }

    /**
     * A function that runs when the user drops a file after dragging it into the drag area.
     * Since onDrop changing will cause a rerender it is recommended that the function in the
     * parent is memoized (useCallback).
     * @param event - The drag event that triggered the function.
     */
    const handleDrop = useCallback((event: DragEvent): void => {

        // Prevent any events in the DOM from being triggered
        event.preventDefault()
        event.stopPropagation()

        setDragging(false)

        if (!event.dataTransfer) return

        if (event.dataTransfer && event.dataTransfer.files.length > 0) {

            // Send the file back to the parent component
            onDrop( event.dataTransfer.files)
        }

    }, [onDrop])

    /**
     * A function that runs when the user drags a file over the drag area.
     * This does nothing, but we need to suppress the default browser behaviour
     * which would be to open the file.
     * @param event - The drag event that triggered the function.
     */
    const handleDragOver = (event: DragEvent) => {

        // Prevent any events in the DOM from being triggered
        event.preventDefault()
        event.stopPropagation()
    }

    /**
     * A hook that adds an event listener after the component mounts and removes it when
     * the component is unmounted.
     */
    useEffect(() => {

        if (dropArea.current != null) {

            // This destructuring is to prevent a code lint bug
            const {current} = dropArea

            current.addEventListener('dragenter', handleDragIn)
            current.addEventListener('dragleave', handleDragOut)
            current.addEventListener('dragover', handleDragOver)
            current.addEventListener('drop', handleDrop)
        }

        // The return statement runs when the component unmounts
        return () => {

            const {current} = dropArea

            if (current) {
                current.removeEventListener('dragenter', handleDragIn)
                current.removeEventListener('dragleave', handleDragOut)
                current.removeEventListener('dragover', handleDragOver)
                current.removeEventListener('drop', handleDrop)
            }
        }

    }, [handleDrop])

    return (

        <div className={`drag-and-drop p-2 d-flex justify-content-center ${className} ${dragging ? "drag-hover" : ""}`}
             ref={dropArea}
        >
            {children}
        </div>
    )
};

DragAndDrop.propTypes = {

    children: PropTypes.element.isRequired,
    className: PropTypes.string,
    onDrop: PropTypes.func.isRequired
};