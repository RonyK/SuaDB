package exception;

import java.io.Serializable;

/**
 * Created by CDS on 2016-12-11.
 */
public class ArrayInputException extends RuntimeException{
	public ArrayInputException(){}

	public ArrayInputException(String e){
		super(e);
	}
}
