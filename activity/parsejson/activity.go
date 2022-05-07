package parsejson

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/data/coerce"
)

// Activity is an activity that converts XML data into JSON object.
// inputs: XML data
// outputs: JSON object
type Activity struct {
}

func init() {
	_ = activity.Register(&Activity{})
}

var activityMd = activity.ToMetadata(&Input{}, &Output{})

// Metadata returns the activity's metadata
func (a *Activity) Metadata() *activity.Metadata {
	return activityMd
}

func (a *Activity) Eval(context activity.Context) (done bool, err error) {

	context.Logger().Debug("Executing ParseJSON activity")

	input := &Input{}
	err = context.GetInputObject(input)
	if err != nil {
		return false, err
	}
	output := &Output{}

	jsonData, err := coerce.ToBytes(input.JsonData)
	if err != nil {
		return false, activity.NewError(fmt.Sprintf("Failed to convert input data to bytes: %s", err.Error()), "", nil)
	}
	var content interface{}
	err = json.NewDecoder(bytes.NewBuffer(jsonData)).Decode(&content)
	if err != nil {
		context.Logger().Error(err)
		return false, activity.NewError(fmt.Sprintf("Failed to convert JSON data: %s", err.Error()), "", nil)
	}

	output.JsonObject, err = coerce.ToObject(content)
	err = context.SetOutputObject(output)
	if err != nil {
		return false, err
	}

	context.Logger().Debug("ParseJSON activity completed")
	return true, nil
}
