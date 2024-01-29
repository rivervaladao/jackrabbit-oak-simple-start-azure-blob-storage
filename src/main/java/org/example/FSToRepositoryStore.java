package org.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributes;
import java.util.Calendar;

import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

public class FSToRepositoryStore {
	public static final double MAX_FILE_SIZE = 1999999999;
	
	public static boolean createNodes(Session session, String fsPath, int level) throws RepositoryException{
		long start = System.nanoTime();
		boolean success = false;
		if (fsPath != null) {
			File directory = new File(fsPath);
			if (directory.isDirectory() && directory.exists() & session != null)
				store(directory.listFiles(),session.getRootNode(),Paths.get(fsPath).getNameCount()+level);
			success = true;
		}
		session.save();
		long end = System.nanoTime();
		System.out.println("=====================================================================");
		System.out.println("Total time taken(in milliseconds) for storage : " +((end -start)/1000000));
		System.out.println("=====================================================================");
		return success;
	}

	private static void store(File[] files, Node node, int level) {
	    for (File file : files) {
	        if (file.isDirectory() && Paths.get(file.getPath()).getNameCount() <= level) {
	            System.out.println("Directory: " + file.getName());	
	            Node added = null;
				try {
					added = node.addNode(file.getName().replaceAll("[^a-zA-Z0-9\\.\\-]", "_"));
					added.setProperty("path", file.getPath());
		            Calendar lasModified = Calendar.getInstance();
		            lasModified.setTimeInMillis(file.lastModified());
		            added.setProperty("modified", lasModified);
				} catch (ItemExistsException e) {
					e.printStackTrace();
					System.out.println("Item already exists!");
				} catch (PathNotFoundException e) {
					e.printStackTrace();
				} catch (VersionException e) {
					e.printStackTrace();
				} catch (ConstraintViolationException e) {
					e.printStackTrace();
				} catch (LockException e) {
					e.printStackTrace();
				} catch (RepositoryException e) {
					e.printStackTrace();
				}	
				if (added == null) {
		            store(file.listFiles(), node, level); // Calls same method again.					
				} else {
		            store(file.listFiles(), added, level); // Calls same method again.
				}
	        } else {
	        	if(file.length() <= MAX_FILE_SIZE) {
	            System.out.println("File: " + file.getName()+" with size : "+file.length());
	            Node added = null;
				try {
					added = node.addNode(file.getName().replaceAll("[^a-zA-Z0-9\\.\\-]", "_"));
					added.setProperty("path", file.getPath());
		            Calendar lasModified = Calendar.getInstance();
		            lasModified.setTimeInMillis(file.lastModified());
		            added.setProperty("modified", lasModified);
		            if(file.canRead()) {
		            	try {
							added.setProperty("file", new FileInputStream(file.getAbsolutePath()));
						} catch (FileNotFoundException e) {
							System.out.println("Cannot read file : "+file.getPath());
							e.printStackTrace();
						}
		            }
		            else
		            	System.out.println("Cannot read file: "+ file.getAbsolutePath());
				} catch (ItemExistsException e) {
					e.printStackTrace();
					System.out.println("Item already exists!");
				} catch (PathNotFoundException e) {
					e.printStackTrace();
				} catch (VersionException e) {
					e.printStackTrace();
				} catch (ConstraintViolationException e) {
					e.printStackTrace();
				} catch (LockException e) {
					e.printStackTrace();
				} catch (RepositoryException e) {
					e.printStackTrace();
				}	            
	        	}
	        }
	    }
	}
	
}
