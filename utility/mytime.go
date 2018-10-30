/**
 * Created by gowinder@hotmail.com on 2018/1/5.
 */

package utility

import "time"

type MyTime struct {
	time.Time
}

func (self *MyTime) UnmarshalJSON(b []byte) (err error) {
	s := string(b)
	t, err := time.Parse(time.RFC3339Nano, s[1:len(s)-1])
	if err != nil {
		t, err = time.Parse("2006-01-02 15:04:05", s[1:len(s)-1])

	}
	self.Time = t
	return
}

func (self MyTime) GetBSON() (interface{}, error) {
	//str := self.Time.Format("2006-01-02 15:04:05")
	return self.Time, nil
}

func (self *MyTime) MarshalJSON() (data []byte, err error) {
	str := "\""+self.Time.Format("2006-01-02 15:04:05")+"\""
	data = []byte(str)
	return data, nil
}