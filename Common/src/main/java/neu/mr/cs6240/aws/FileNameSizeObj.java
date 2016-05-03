package neu.mr.cs6240.aws;

/**
 * This is a POJO class for the S3 Files which holds file name and size
 * @author prasad memane
 * @author swapnil mahajan
 */
public class FileNameSizeObj implements Comparable<FileNameSizeObj>{

	private String name;
	private Long size;
	
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getSize() {
		return size;
	}
	public void setSize(Long size) {
		this.size = size;
	}
	
	public FileNameSizeObj(String name, Long size) {
		this.name = name;
		this.size = size;
	}
	@Override
	public int compareTo(FileNameSizeObj o) {
		return this.size.compareTo(o.getSize());
	}
		
}
