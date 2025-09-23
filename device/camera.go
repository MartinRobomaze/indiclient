package device

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/goastro/indiclient"
)

type CameraDevice struct {
	indiClient *indiclient.INDIClient
	deviceName string
}

type CameraInfo struct {
	Width        int
	Height       int
	PixelSize    float64
	ExposureTime float64
	Temperature  float64
	Gain         float64
	Offset       int
}

func NewCameraDevice(indiClient *indiclient.INDIClient, deviceName string) *CameraDevice {
	return &CameraDevice{
		indiClient: indiClient,
		deviceName: deviceName,
	}
}

func (c *CameraDevice) Connect(ctx context.Context) error {
	done := make(chan bool)
	c.indiClient.RegisterPropertyUpdatedHandler(c.deviceName, PropertyConnection, func(prop indiclient.Property) {
		if prop.PropertyType() != indiclient.PropertyTypeSwitch {
			return
		}

		propSwitch := prop.(*indiclient.SwitchProperty)
		if propSwitch.State == indiclient.PropertyStateOk {
			done <- true
		}
	})

	err := c.indiClient.SetSwitchValue(ctx, c.deviceName, "CONNECTION", "CONNECT", indiclient.SwitchStateOn)
	if err != nil {
		return fmt.Errorf("error connecting to camera: %w", err)
	}

	err = c.indiClient.GetProperties(c.deviceName, "")
	if err != nil {
		return fmt.Errorf("error loading camera properties: %w", err)
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = c.indiClient.WaitForPropsUpdateOrCancel(ctxTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to camera: %w", err)
	}

	defer c.indiClient.RemovePropertyUpdatedHandler(c.deviceName, PropertyConnection)
	select {
	case <-done:
	case <-time.After(time.Second * time.Duration(30)):
		return fmt.Errorf("timed out waiting for camera to connect")
	}

	err = c.indiClient.EnableBlob(c.deviceName, PropertyCcd1, indiclient.BlobEnableAlso)
	if err != nil {
		return fmt.Errorf("error connecting to camera: %w", err)
	}

	return nil
}

func (c *CameraDevice) GetInfo() (*CameraInfo, error) {
	err := c.indiClient.GetProperties(c.deviceName, "")
	if err != nil {
		return nil, fmt.Errorf("error loading camera properties: %w", err)
	}

	cameraDevice, ok := c.indiClient.Device(c.deviceName)
	if !ok {
		return nil, fmt.Errorf("device %s not found", c.deviceName)
	}

	frame := cameraDevice.NumberProperties[PropertyCcdFrame].Values
	width, err := getAndParseNumberValue[int](frame, ValueCcdFrameX)
	if err != nil {
		return nil, err
	}

	height, err := getAndParseNumberValue[int](frame, ValueCcdFrameY)
	if err != nil {
		return nil, err
	}

	ccdInfo := cameraDevice.NumberProperties[PropertyCcdInfo].Values
	pixelSize, err := getAndParseNumberValue[float64](ccdInfo, ValueCcdPixelSizeName)
	if err != nil {
		return nil, err
	}

	expTime := cameraDevice.NumberProperties[PropertyCcdExposure].Values
	exposure, err := getAndParseNumberValue[float64](expTime, ValueCcdExposureName)
	if err != nil {
		return nil, err
	}

	temperatureProp := cameraDevice.NumberProperties[PropertyCcdTemperature].Values
	temperature, err := getAndParseNumberValue[float64](temperatureProp, ValueCcdTemperatureName)
	if err != nil {
		return nil, err
	}

	gainProp := cameraDevice.NumberProperties[PropertyCcdGain].Values
	gain, err := getAndParseNumberValue[float64](gainProp, ValueCcdGainName)
	if err != nil {
		return nil, err
	}

	offsetProp := cameraDevice.NumberProperties[PropertyCcdOffset].Values
	offset, err := getAndParseNumberValue[int](offsetProp, ValueCcdOffsetName)
	if err != nil {
		return nil, err
	}

	return &CameraInfo{
		Width:        width,
		Height:       height,
		PixelSize:    pixelSize,
		ExposureTime: exposure,
		Temperature:  temperature,
		Gain:         gain,
		Offset:       offset,
	}, nil
}

func (c *CameraDevice) SetGain(ctx context.Context, gain float64) error {
	err := c.indiClient.SetNumberValue(
		ctx, c.deviceName, PropertyCcdGain, ValueCcdGainName, strconv.FormatFloat(gain, 'f', -1, 64))
	if err != nil {
		return fmt.Errorf("error setting camera gain: %w", err)
	}

	return nil
}

func (c *CameraDevice) Expose(ctx context.Context, exposureTime float64) (rdr io.ReadCloser, filename string, length int64, err error) {
	err = c.indiClient.SetNumberValue(
		ctx, c.deviceName, PropertyCcdExposure, ValueCcdExposureName, strconv.FormatFloat(exposureTime, 'f', -1, 64))
	if err != nil {
		return nil, "", 0, fmt.Errorf("error setting camera exposure: %w", err)
	}

	done := make(chan bool)
	c.indiClient.RegisterPropertyUpdatedHandler(c.deviceName, PropertyCcdExposure, func(prop indiclient.Property) {
		if prop.PropertyType() != indiclient.PropertyTypeNumber {
			return
		}

		propSwitch := prop.(*indiclient.NumberProperty)
		if propSwitch.State == indiclient.PropertyStateOk {
			done <- true
		}
	})

	select {
	case <-done:
	case <-time.After(time.Duration(exposureTime)*time.Second + 10*time.Second):
		return nil, "", 0, fmt.Errorf("timed out waiting for camera to exposure")
	}

	c.indiClient.RemovePropertyUpdatedHandler(c.deviceName, PropertyConnection)

	time.Sleep(2 * time.Second)

	rdr, filename, length, err = c.indiClient.GetBlob(c.deviceName, "CCD1", "CCD1")
	if err != nil {
		return nil, "", 0, fmt.Errorf("error loading camera properties: %w", err)
	}

	return rdr, filename, length, nil
}

type ImageResult struct {
	Stream   io.ReadCloser
	Filename string
	Length   int64
}

func (c *CameraDevice) ExposeAsync(ctx context.Context, exposureTime float64) (<-chan ImageResult, <-chan error) {
	imgRes := make(chan ImageResult)
	errChan := make(chan error)
	err := c.indiClient.SetNumberValue(
		ctx, c.deviceName, PropertyCcdExposure, ValueCcdExposureName, strconv.FormatFloat(exposureTime, 'f', -1, 64))
	if err != nil {
		go func() {
			errChan <- err
		}()
		return imgRes, errChan
	}

	go func() {
		done := make(chan bool)
		c.indiClient.RegisterPropertyUpdatedHandler(c.deviceName, PropertyCcdExposure, func(prop indiclient.Property) {
			if prop.PropertyType() != indiclient.PropertyTypeNumber {
				return
			}

			propSwitch := prop.(*indiclient.NumberProperty)
			if propSwitch.State == indiclient.PropertyStateOk {
				done <- true
			}
		})

		select {
		case <-done:
		case <-ctx.Done():
		case <-time.After(time.Duration(exposureTime)*time.Second + 10*time.Second):
			errChan <- fmt.Errorf("timed out waiting for camera to exposure")
			return
		}

		c.indiClient.RemovePropertyUpdatedHandler(c.deviceName, PropertyConnection)

		if ctx.Err() != nil {
			errChan <- ctx.Err()
			return
		}

		rdr, filename, length, err := c.indiClient.GetBlob(c.deviceName, "CCD1", "CCD1")
		if err != nil {
			errChan <- fmt.Errorf("error loading camera properties: %w", err)
			return
		}

		imgRes <- ImageResult{
			Stream:   rdr,
			Filename: filename,
			Length:   length,
		}
	}()

	return imgRes, errChan
}

func getAndParseNumberValue[T int | float64](values map[string]indiclient.NumberValue, propertyName string) (T, error) {
	heightStr := values[propertyName].Value
	height, err := strconv.ParseFloat(heightStr, 64)
	if err != nil {
		return 0, err
	}

	return T(height), nil
}
